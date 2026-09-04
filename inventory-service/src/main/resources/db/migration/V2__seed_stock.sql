-- Demo catalogue. LOW-STOCK-1 exists specifically so the failure branch of the
-- saga can be demonstrated without editing the database by hand.
INSERT INTO stock_items (sku, on_hand, reserved) VALUES
    ('WIDGET-BLUE',   100, 0),
    ('WIDGET-RED',     50, 0),
    ('GIZMO-LARGE',    25, 0),
    ('GIZMO-SMALL',   200, 0),
    ('LOW-STOCK-1',     1, 0);
