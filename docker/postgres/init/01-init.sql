-- Create admin user first
CREATE USER admin WITH PASSWORD 'admin123';
ALTER USER admin WITH SUPERUSER;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Grant schema privileges to admin
GRANT ALL PRIVILEGES ON DATABASE ecommerce_dw TO admin;
GRANT ALL PRIVILEGES ON SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON SCHEMA raw TO admin;
GRANT ALL PRIVILEGES ON SCHEMA staging TO admin;
GRANT ALL PRIVILEGES ON SCHEMA warehouse TO admin;

-- Raw Tables
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    username VARCHAR(100),
    email VARCHAR(255),
    gender VARCHAR(1),
    birthdate DATE,
    device_type VARCHAR(50),
    device_id VARCHAR(100),
    device_version VARCHAR(50),
    home_location_lat DOUBLE PRECISION,
    home_location_long DOUBLE PRECISION,
    home_location VARCHAR(100),
    home_country VARCHAR(100),
    first_join_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.products (
    id INTEGER PRIMARY KEY,
    gender VARCHAR(50),
    master_category VARCHAR(100),
    sub_category VARCHAR(100),
    article_type VARCHAR(100),
    base_colour VARCHAR(50),
    season VARCHAR(50),
    year INTEGER,
    usage VARCHAR(50),
    product_display_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.transactions (
    created_at TIMESTAMP,
    customer_id INTEGER,
    booking_id VARCHAR(100) PRIMARY KEY,
    session_id VARCHAR(100),
    product_metadata JSONB,
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    promo_amount INTEGER,
    promo_code VARCHAR(50),
    shipment_fee INTEGER,
    shipment_date_limit TIMESTAMP,
    shipment_location_lat DOUBLE PRECISION,
    shipment_location_long DOUBLE PRECISION,
    total_amount INTEGER
);

CREATE TABLE IF NOT EXISTS raw.click_stream (
    session_id VARCHAR(100),
    event_name VARCHAR(50),
    event_time TIMESTAMP,
    event_id VARCHAR(100) PRIMARY KEY,
    traffic_source VARCHAR(50),
    event_metadata JSONB
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_customer_id ON raw.transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_session_id ON raw.click_stream(session_id);
CREATE INDEX IF NOT EXISTS idx_event_time ON raw.click_stream(event_time);
CREATE INDEX IF NOT EXISTS idx_transaction_time ON raw.transactions(created_at);

-- Grant table permissions to admin
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO admin;