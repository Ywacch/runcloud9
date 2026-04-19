-- Schema (must match images/ecommerce/db.go InitSchema)
CREATE TABLE IF NOT EXISTS products (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    stock INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    id         SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES products(id),
    quantity   INT NOT NULL DEFAULT 1,
    status     TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_product ON orders(product_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- 1,000 products
INSERT INTO products (name, price, stock)
SELECT
    (ARRAY[
        'Wireless', 'Premium', 'Essential', 'Pro', 'Ultra',
        'Classic', 'Compact', 'Advanced', 'Elite', 'Portable'
    ])[1 + (random() * 9)::int]
    || ' ' ||
    (ARRAY[
        'Headphones', 'Keyboard', 'Monitor', 'Backpack', 'Charger',
        'Speaker', 'Mouse', 'Webcam', 'Microphone', 'Hub',
        'Cable', 'Stand', 'Case', 'Adapter', 'Dock',
        'Tablet', 'Watch', 'Lamp', 'Drive', 'Router'
    ])[1 + (random() * 19)::int]
    || ' ' || i,
    ROUND((4.99 + random() * 495.00)::numeric, 2),
    10000
FROM generate_series(1, 1000) AS i;

-- 50,000 historical fulfilled orders
INSERT INTO orders (product_id, quantity, status, created_at)
SELECT
    1 + (random() * 999)::int,
    1 + (random() * 4)::int,
    'fulfilled',
    NOW() - (random() * interval '30 days')
FROM generate_series(1, 50000);
