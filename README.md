# OrderFlow

Three Spring Boot services that place an order across a payment service and an inventory service **without a distributed transaction** — and correctly undo themselves when a step fails partway through.

The interesting case is not the happy path. It is this one:

> Stock has been reserved. The card is then declined. The order must be cancelled **and** the stock must go back — and it must still be correct if the process crashes at any point in between, or if the broker delivers the same message twice.

Java 17 · Spring Boot 3.3 · Kafka · PostgreSQL · Testcontainers

---

## Contents

- [The two problems](#the-two-problems)
- [Problem 1: the dual-write problem](#problem-1-the-dual-write-problem)
- [Problem 2: consistency without a distributed transaction](#problem-2-consistency-without-a-distributed-transaction)
- [Delivery semantics, stated honestly](#delivery-semantics-stated-honestly)
- [Architecture](#architecture)
- [Running it](#running-it)
- [Watching a saga compensate](#watching-a-saga-compensate)
- [Tests](#tests)
- [Design decisions](#design-decisions)
- [What I would do next](#what-i-would-do-next)

---

## The two problems

Splitting a monolith into services replaces one database transaction with a distributed problem that has no equally tidy answer. Two things break immediately, and this project is built around solving both.

---

## Problem 1: the dual-write problem

A service saves an order and publishes an event. That is **two writes to two systems with no transaction spanning them**, and every ordering of the two is wrong somewhere:

```java
// Publish first: if the commit fails, other services act on an order
// that does not exist. Inventory sets stock aside for nothing.
kafka.send(new OrderCreated(order));
orderRepository.save(order);            // ← crash here

// Commit first: if the publish fails, the order exists and nothing
// happens to it. It sits in PENDING forever and nobody is waiting.
orderRepository.save(order);
kafka.send(new OrderCreated(order));    // ← crash here
```

There is no third ordering that works, and no amount of retrying fixes it: the process can die between any two statements.

### The transactional outbox

Write the message **to the same database, in the same transaction** as the state change. A separate relay publishes it afterwards.

```java
@Transactional
public OrderResponse create(CreateOrderRequest request) {
    orders.saveAndFlush(order);                     // state change
    outbox.enqueue(sagaId, ..., "ReserveInventory", command);  // message
}                                                   // both commit, or neither does
```

The two writes now go to one system, so they are atomic by construction. If the transaction rolls back, the message never existed. If it commits, the message *will* be delivered, however many times the process crashes first.

`OutboxWriter` declares `Propagation.MANDATORY` for exactly this reason. Calling it outside a transaction throws instead of quietly committing on its own — which would silently degrade the guarantee back into "publish regardless of whether the state change succeeded", and only show up under load. There is a test asserting that it throws.

---

## Problem 2: consistency without a distributed transaction

Confirming an order means reserving stock in one service and taking money in another, across two databases. Two-phase commit could in principle span them, but it holds locks across a network for the duration and makes every participant's availability depend on every other's — a coordinator that dies mid-decision leaves resources locked until a human intervenes.

### An orchestrated saga

Each step commits locally and immediately. If a later step fails, earlier steps are undone by explicit **compensating** actions.

```
  PENDING ──InventoryReserved──▶ INVENTORY_RESERVED ──PaymentAuthorized──▶ CONFIRMED
     │                                    │
     │ ReservationFailed                  │ PaymentFailed
     │ (nothing to undo)                  │ → emits ReleaseInventory
     ▼                                    ▼
  CANCELLED ◀────InventoryReleased──── COMPENSATING
```

Two details in that diagram carry most of the weight:

**`COMPENSATING` is a real state, not a moment.** When payment fails, the order does *not* go straight to `CANCELLED`. It sits in `COMPENSATING` until inventory confirms the release. A crash in that window leaves the true state visible in the database — stock is still held — rather than a cancelled order with inventory quietly stranded against it.

**The left branch has no compensation, deliberately.** If stock was never reserved, there is nothing to give back. Emitting a release there would be a bug rather than extra safety: the inventory service would decrement a reservation that never existed. There is a test asserting no release is emitted on that path.

### Orchestration, not choreography

The alternative is choreography — each service listens for the previous one's event and decides for itself. That needs no coordinator, but the flow exists only as an emergent property of who happens to subscribe to what, and answering *"why did order 41 stall?"* means reading every service in the system. Here the flow is one readable state machine in `OrderSaga`, and the cost is that this class knows the names of the steps.

---

## Delivery semantics, stated honestly

**The outbox relay is at-least-once, and cannot be anything else.** The window is unavoidable: the broker acknowledges a message, then the process dies before the row is marked published. On restart it goes out again. Two systems, no shared transaction — the same problem the outbox solved for the *first* write reappears for the second.

So the duplicate is not prevented. It is made **harmless**:

| Layer | Mechanism | Guards against |
|---|---|---|
| Transport | `messageId` header + `processed_messages` table | The same message delivered twice |
| Business | `idempotency_key` unique on `payments` | The same command arriving under a *different* message id |
| Saga | Every handler re-checks the status it expects | A late or out-of-order reply |

The dedup record and the work it guards **share one transaction**. That is what makes it safe rather than merely likely to work: the guard cannot commit for work that rolled back, and the work cannot commit without the guard.

At-least-once delivery plus idempotent consumers gives exactly-once *effect*, which is the only form of exactly-once that exists in a distributed system.

### Ordering

Every message is keyed by `sagaId`, so all messages for one order land on one partition and are delivered in the order they were written. Keying by anything else would let a compensation overtake the action it compensates. The relay publishes in insertion order and **synchronously** — sending the batch asynchronously would let message 7 reach the broker before message 3, undoing the ordering the id-ordered query just established.

### When a message cannot be processed

Kafka has no per-message redelivery. A consumer that keeps failing on record N blocks its entire partition, and every order behind it stops moving. Retrying forever therefore does not make the system more reliable — it converts one broken message into a stalled partition.

So attempts are bounded (exponential backoff, 30s ceiling) and the message is then parked on a dead-letter topic with its original topic and failure reason in headers. Losing one order to manual triage beats freezing all of them. A payload that cannot be deserialised skips retries entirely, since it will never parse.

---

## Architecture

```
                       ┌──────────────────┐
   POST /orders ──────▶│  order-service   │  owns orders + the saga
                       │  saga + outbox   │
                       └────────┬─────────┘
                                │
        commands ───────────────┼─────────────── events
                                │
     orderflow.commands.inventory│orderflow.events.inventory
     orderflow.commands.payment  │orderflow.events.payment
                                │
              ┌─────────────────┴─────────────────┐
              ▼                                   ▼
      ┌───────────────┐                  ┌────────────────┐
      │payment-service│                  │inventory-service│
      │  own database │                  │  own database   │
      └───────────────┘                  └────────────────┘
```

**Commands and events are separate topics**, because they are different kinds of message:

- A **command** is addressed to one service and tells it to do something. It may be refused. `ReserveInventory` is a request.
- An **event** is a statement of fact addressed to nobody in particular. `InventoryReserved` already happened and cannot be refused; any number of services may care, or none.

The practical consequence: a service owns its command topic, while anyone may subscribe to an event topic without the publisher knowing. Adding a consumer to an event topic requires no change to the service that emits it.

### Modules

```
common-events/      The message contract. Sealed interfaces + records, so the
                    set of messages is closed and a switch over it is checked
                    by the compiler.
common-messaging/   Outbox, relay, idempotent consumption, Kafka error handling.
                    Extracted because all three services need exactly this, and
                    copying it three times is how three subtly different
                    versions come to exist.
order-service/      Orders, the saga orchestrator, the REST API.
payment-service/    Authorise, decline, refund. Simulated gateway.
inventory-service/  Reserve and release stock.
```

**Why sealed interfaces for the contract.** Adding a fourth inventory command makes every handler that fails to account for it stop compiling. The alternative — a string `type` field and an `if` chain — fails at runtime by silently ignoring the message, which is discovered days later when someone asks why a saga stalled.

---

## Running it

```bash
docker compose up --build
```

| | |
|---|---|
| Order API | http://localhost:8081/api/v1/orders |
| Swagger UI | http://localhost:8081/swagger-ui.html |
| Payment service | http://localhost:8082/actuator/health |
| Inventory service | http://localhost:8083/actuator/health |

Seeded stock: `WIDGET-BLUE` (100), `WIDGET-RED` (50), `GIZMO-LARGE` (25), `GIZMO-SMALL` (200), and `LOW-STOCK-1` (1) — which exists specifically so the out-of-stock branch can be demonstrated without editing the database.

For dashboards: `docker compose --profile observability up` (Grafana on :3000, Prometheus on :9090).

---

## Watching a saga compensate

The failure path is the point of the project, and it can be triggered on demand. Under Compose the payment service declines anything over **50000 cents**.

**A successful order:**

```bash
curl -s -X POST localhost:8081/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerRef":"alice","lines":[{"sku":"WIDGET-BLUE","quantity":2,"unitPriceCents":9900}]}' | jq
```

Returns `202 Accepted` with status `PENDING` — the API does not wait for the saga, because making the caller wait would couple its response time to two other services and its availability to theirs. Poll it:

```bash
curl -s localhost:8081/api/v1/orders/1 | jq '.status, .paymentRef'
# "CONFIRMED"  "TXN-..."
```

**An order that compensates** — total is 60000, over the decline ceiling:

```bash
curl -s -X POST localhost:8081/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerRef":"bob","lines":[{"sku":"GIZMO-LARGE","quantity":2,"unitPriceCents":30000}]}' | jq
```

Watch it move `PENDING → INVENTORY_RESERVED → COMPENSATING → CANCELLED`:

```bash
watch -n1 "curl -s localhost:8081/api/v1/orders/2 | jq '.status, .failureReason'"
```

And confirm the stock actually came back rather than being stranded:

```bash
docker compose exec postgres psql -U orderflow -d orderflow_inventory \
  -c "SELECT sku, on_hand, reserved FROM stock_items WHERE sku='GIZMO-LARGE';"
# reserved is back to 0
```

Any `customerRef` containing `DECLINE` is also always refused, if you want the failure without the arithmetic.

---

## Tests

```bash
./mvnw verify
```

20 tests against **real Postgres and real Kafka** via Testcontainers. Embedded Kafka would be faster, but what is under test here is partitioning, consumer groups, offset commits and redelivery — the things an in-memory stand-in approximates rather than implements. Without Docker the integration tests skip rather than fail.

The saga tests drive everything **through the bus**: orders go in through the service, replies come back over Kafka, and assertions are made on the database and on messages actually published. So they cover the outbox relay, serialisation, headers and consumer wiring as well as the state machine.

| Test | Asserts |
|---|---|
| `SagaFlowIT.happyPath` | Reserved → authorised → `CONFIRMED`, with the right amount and a stable idempotency key |
| `SagaFlowIT.compensatesWhenPaymentFailsAfterReservation` | Decline after reservation emits `ReleaseInventory`, holds at `COMPENSATING`, reaches `CANCELLED` only once released |
| `SagaFlowIT.cancelsWithoutCompensationWhenStockIsUnavailable` | Out of stock cancels with **no** release and **no** payment attempt |
| `SagaFlowIT.duplicateReplyIsIgnored` | The same message id twice produces one authorisation, not two |
| `OutboxIT.messagesCommitWithTheOrder` | Order and both messages commit together, in order |
| `OutboxIT.rollbackLeavesNoMessage` | A rolled-back transaction leaves no order **and** no message |
| `OutboxIT.enqueueOutsideATransactionIsRefused` | `MANDATORY` propagation actually throws |
| `PaymentServiceIT.doesNotChargeTwice` | A redelivered command produces one payment row and one charge |
| `InventoryServiceIT.releaseIsIdempotent` | A duplicate release does not invent stock |
| `InventoryServiceIT.partialAvailabilityReservesNothing` | Multi-line orders are all-or-nothing |

---

## Design decisions

**`202 Accepted`, not `201 Created`.** The order resource exists, but the work it represents has only been accepted. Returning "Created" would invite clients to treat a `PENDING` order as a confirmed sale.

**Saga ids in the MDC.** Every log line emitted while handling a message carries its saga id, including lines from deep inside the orchestrator. Reconstructing one order's journey across three services costs one query instead of a manual join across timestamps.

**Optimistic locking on `Order` and `StockItem`.** Replies arrive from independent consumers and nothing stops a delayed `PaymentFailed` landing beside a retried `InventoryReserved`. The loser fails loudly and retries against fresh state instead of overwriting a transition it never saw. Pessimistic locks would serialise every order touching a popular SKU to avoid a collision that is rare and cheap to retry.

**`onHand` and `reserved` tracked separately.** Reserved stock is physically still in the warehouse — promised, not shipped. Collapsing them into one number makes "how much do we have?" and "how much can we sell?" indistinguishable, and a compensating release indistinguishable from a restock.

**A deterministic payment gateway.** A random decline rate would make the compensation test flaky, and a flaky test guarding the most important branch of the saga is worse than no test: it teaches people to rerun until green.

**One Postgres instance, one database per service.** Real deployments give each service its own instance. For local development the isolation that matters is that no service can read another's tables, and separate databases provide that at a fraction of the memory. Noted rather than hidden.

---

## What I would do next

Honest list of what is deliberately not here.

- **Debezium instead of polling.** The relay polls every 500ms, which costs a query per interval per service and adds latency. Tailing the write-ahead log removes both. Polling was chosen because it is self-contained and its failure modes are visible in one file.
- **A saga timeout.** If a participant never replies, the order sits in `PENDING` forever. Production needs a scheduled sweep that fails sagas stuck past a deadline and compensates whatever they had already done.
- **A dead-letter triage tool.** Messages land on `orderflow.dlt` with enough context to act on, but replaying them is currently a manual job.
- **Circuit breakers on the gateway call.** Once `callGateway` is a real HTTP call, a provider outage would exhaust the consumer thread pool. Resilience4j around that boundary is the standard answer.
- **Contract tests between services.** The shared `common-events` module means a breaking change to a record compiles everywhere or nowhere, which is a real benefit — but it is also coupling. Once services deploy independently, that guarantee needs Pact or a schema registry instead.
- **Real load testing.** The correctness properties are proved; throughput is not measured, and I would rather say so than quote numbers I have not taken.
