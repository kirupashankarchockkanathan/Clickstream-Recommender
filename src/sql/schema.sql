-- Relational (structured) side of the platform. See src/python/build_tables.py for what is real vs synthetic.
DROP TABLE IF EXISTS order_items, orders, inventory, products, users CASCADE;
CREATE TABLE users      (user_id BIGINT PRIMARY KEY, region TEXT, segment TEXT, signup_date DATE);            -- synthetic profile
CREATE TABLE products   (product_id BIGINT PRIMARY KEY, category_id BIGINT, category_code TEXT, brand TEXT, last_price NUMERIC(12,2)); -- real
CREATE TABLE inventory  (product_id BIGINT PRIMARY KEY, warehouse TEXT, stock_qty INT);                       -- synthetic
CREATE TABLE orders     (order_id BIGINT PRIMARY KEY, user_id BIGINT, user_session TEXT, order_time TEXT);    -- real (from purchase events)
CREATE TABLE order_items(order_id BIGINT, product_id BIGINT, price NUMERIC(12,2));                            -- real
