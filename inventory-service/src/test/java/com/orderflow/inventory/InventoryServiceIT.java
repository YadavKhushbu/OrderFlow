package com.orderflow.inventory;

import com.orderflow.events.InventoryCommand;
import com.orderflow.events.OrderLine;
import com.orderflow.inventory.support.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reservation and release, the two halves of the inventory side of the saga.
 *
 * <p>The interesting cases are all about repetition. Delivery is at-least-once,
 * so both commands genuinely will arrive more than once, and stock counters that
 * drift under redelivery are the difference between a warehouse that balances
 * and one that does not.
 */
@AbstractIntegrationTest.IntegrationTest
@DisplayName("Inventory reservation")
class InventoryServiceIT extends AbstractIntegrationTest {

    @Autowired InventoryService inventory;
    @Autowired StockRepository stock;
    @Autowired ReservationRepository reservations;
    @Autowired JdbcTemplate jdbc;

    @Test
    @DisplayName("Reserving moves stock from available to reserved without changing on-hand")
    void reserveHoldsStock() {
        int availableBefore = available("WIDGET-BLUE");
        int onHandBefore = onHand("WIDGET-BLUE");
        UUID sagaId = UUID.randomUUID();

        inventory.reserve(new InventoryCommand.Reserve(sagaId, 1L,
                List.of(new OrderLine("WIDGET-BLUE", 3, 100L))));

        assertThat(available("WIDGET-BLUE")).isEqualTo(availableBefore - 3);
        // The goods have not moved; they are promised, not shipped. Decrementing
        // on-hand here would misreport what is physically in the warehouse and
        // make a compensating release indistinguishable from a restock.
        assertThat(onHand("WIDGET-BLUE"))
                .as("a reservation must not change what is physically in stock")
                .isEqualTo(onHandBefore);
        assertThat(messageTypes(sagaId)).contains("InventoryReserved");
    }

    @Test
    @DisplayName("Insufficient stock is refused, and nothing is held")
    void insufficientStockIsRefused() {
        int before = available("LOW-STOCK-1");
        UUID sagaId = UUID.randomUUID();

        inventory.reserve(new InventoryCommand.Reserve(sagaId, 2L,
                List.of(new OrderLine("LOW-STOCK-1", before + 50, 100L))));

        assertThat(available("LOW-STOCK-1")).isEqualTo(before);
        assertThat(messageTypes(sagaId))
                .containsExactly("InventoryReservationFailed");
        assertThat(reservations.findWithLinesBySagaId(sagaId))
                .as("a refused command must not leave a reservation row behind")
                .isEmpty();
    }

    @Test
    @DisplayName("A multi-line order is all-or-nothing")
    void partialAvailabilityReservesNothing() {
        int widgets = available("WIDGET-RED");
        UUID sagaId = UUID.randomUUID();

        // One line is satisfiable, the other is not.
        inventory.reserve(new InventoryCommand.Reserve(sagaId, 3L, List.of(
                new OrderLine("WIDGET-RED", 1, 100L),
                new OrderLine("LOW-STOCK-1", 9999, 100L))));

        assertThat(available("WIDGET-RED"))
                .as("a partial hold would strand stock the order can never use")
                .isEqualTo(widgets);
        assertThat(messageTypes(sagaId)).containsExactly("InventoryReservationFailed");
    }

    @Test
    @DisplayName("A redelivered reserve command does not hold stock twice")
    void reserveIsIdempotent() {
        int before = available("GIZMO-LARGE");
        UUID sagaId = UUID.randomUUID();
        var command = new InventoryCommand.Reserve(sagaId, 4L,
                List.of(new OrderLine("GIZMO-LARGE", 2, 100L)));

        inventory.reserve(command);
        inventory.reserve(command);

        assertThat(available("GIZMO-LARGE"))
                .as("two deliveries of one command must hold stock once")
                .isEqualTo(before - 2);
        // The reply is sent again though, because a lost reply is the most likely
        // reason the command was redelivered at all.
        assertThat(messageTypes(sagaId).stream().filter("InventoryReserved"::equals).count())
                .isEqualTo(2);
    }

    @Test
    @DisplayName("Releasing returns stock, and releasing twice does not over-credit")
    void releaseIsIdempotent() {
        int before = available("GIZMO-SMALL");
        UUID sagaId = UUID.randomUUID();

        inventory.reserve(new InventoryCommand.Reserve(sagaId, 5L,
                List.of(new OrderLine("GIZMO-SMALL", 4, 100L))));
        assertThat(available("GIZMO-SMALL")).isEqualTo(before - 4);

        inventory.release(new InventoryCommand.Release(sagaId, 5L, "payment declined"));
        assertThat(available("GIZMO-SMALL")).isEqualTo(before);

        inventory.release(new InventoryCommand.Release(sagaId, 5L, "payment declined"));
        assertThat(available("GIZMO-SMALL"))
                .as("a duplicate release must not invent stock that does not exist")
                .isEqualTo(before);
    }

    @Test
    @DisplayName("Releasing an unknown saga is acknowledged rather than ignored")
    void releaseOfUnknownSagaStillReplies() {
        UUID sagaId = UUID.randomUUID();

        inventory.release(new InventoryCommand.Release(sagaId, 99L, "compensation"));

        // Silence would leave the order saga waiting in COMPENSATING forever for
        // a reply that is never coming.
        assertThat(messageTypes(sagaId)).containsExactly("InventoryReleased");
    }

    // ------------------------------------------------------------------ helpers

    private int available(String sku) {
        StockItem item = stock.findById(sku).orElseThrow();
        return item.available();
    }

    private int onHand(String sku) {
        return stock.findById(sku).orElseThrow().getOnHand();
    }

    private List<String> messageTypes(UUID sagaId) {
        return jdbc.queryForList(
                "SELECT message_type FROM outbox_events WHERE saga_id = ? ORDER BY id",
                String.class, sagaId);
    }
}
