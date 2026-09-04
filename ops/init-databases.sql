-- One Postgres instance hosting a database per service.
--
-- Real deployments give each service its own instance: sharing a server means a
-- runaway query in one service can starve the others, and it makes the "no
-- shared database" rule a matter of discipline rather than of physics. For local
-- development the isolation that matters is that no service can read another's
-- tables, and separate databases provide that at a fraction of the memory.
CREATE DATABASE orderflow_orders;
CREATE DATABASE orderflow_payments;
CREATE DATABASE orderflow_inventory;
