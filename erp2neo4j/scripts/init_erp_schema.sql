-- Sample ERP schema for testing erp2neo4j
-- Run via docker-compose or psql

CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    location      VARCHAR(100)
);

CREATE TABLE employees (
    employee_id   SERIAL PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    email         VARCHAR(255) UNIQUE,
    department_id INT REFERENCES departments(department_id),
    role          VARCHAR(100),
    hire_date     DATE,
    updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE categories (
    category_id        SERIAL PRIMARY KEY,
    name               VARCHAR(100) NOT NULL,
    parent_category_id INT REFERENCES categories(category_id)
);

CREATE TABLE suppliers (
    supplier_id    SERIAL PRIMARY KEY,
    name           VARCHAR(200) NOT NULL,
    contact_email  VARCHAR(255),
    country        VARCHAR(100),
    lead_time_days INT,
    updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    email       VARCHAR(255) UNIQUE,
    phone       VARCHAR(50),
    address     TEXT,
    city        VARCHAR(100),
    country     VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW(),
    deleted_at  TIMESTAMP
);

CREATE TABLE products (
    product_id      SERIAL PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    sku             VARCHAR(100) UNIQUE,
    description     TEXT,
    unit_price      DECIMAL(12,2),
    stock_quantity  INT DEFAULT 0,
    category_id     INT REFERENCES categories(category_id),
    supplier_id     INT REFERENCES suppliers(supplier_id),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(customer_id),
    employee_id  INT REFERENCES employees(employee_id),
    status       VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(14,2),
    currency     CHAR(3) DEFAULT 'USD',
    order_date   TIMESTAMP DEFAULT NOW(),
    shipped_date TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- Junction table: order_items (becomes CONTAINS relationship)
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INT NOT NULL REFERENCES orders(order_id),
    product_id    INT NOT NULL REFERENCES products(product_id),
    quantity      INT NOT NULL,
    unit_price    DECIMAL(12,2),
    discount      DECIMAL(5,2) DEFAULT 0
);

CREATE TABLE invoices (
    invoice_id SERIAL PRIMARY KEY,
    order_id   INT NOT NULL REFERENCES orders(order_id),
    amount     DECIMAL(14,2),
    status     VARCHAR(50) DEFAULT 'unpaid',
    due_date   DATE,
    paid_at    TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Junction table: supplier_contracts (becomes USES_CONTRACT relationship)
CREATE TABLE supplier_contracts (
    contract_id    SERIAL PRIMARY KEY,
    supplier_id    INT NOT NULL REFERENCES suppliers(supplier_id),
    product_id     INT NOT NULL REFERENCES products(product_id),
    contract_price DECIMAL(12,2),
    valid_from     DATE,
    valid_to       DATE
);

-- Indexes for watermark-based CDC
CREATE INDEX idx_customers_updated_at  ON customers(updated_at);
CREATE INDEX idx_orders_updated_at     ON orders(updated_at);
CREATE INDEX idx_products_updated_at   ON products(updated_at);
CREATE INDEX idx_invoices_updated_at   ON invoices(updated_at);
CREATE INDEX idx_employees_updated_at  ON employees(updated_at);
CREATE INDEX idx_suppliers_updated_at  ON suppliers(updated_at);

-- Sample seed data
INSERT INTO departments (name, location) VALUES
    ('Engineering', 'Lahore'), ('Sales', 'Karachi'), ('Finance', 'Islamabad');

INSERT INTO categories (name) VALUES
    ('Electronics'), ('Clothing'), ('Food & Beverage'), ('Industrial');

INSERT INTO suppliers (name, contact_email, country, lead_time_days) VALUES
    ('TechSupply Co.', 'supply@techco.com', 'China', 14),
    ('GlobalParts Ltd.', 'orders@globalparts.com', 'Germany', 21);

INSERT INTO customers (name, email, city, country) VALUES
    ('Acme Corp', 'acme@example.com', 'Lahore', 'PK'),
    ('Beta LLC',  'beta@example.com', 'Karachi', 'PK');

INSERT INTO products (name, sku, unit_price, category_id, supplier_id) VALUES
    ('Laptop Pro 15', 'LP-001', 1299.99, 1, 1),
    ('Wireless Mouse', 'WM-042', 29.99,  1, 1),
    ('Office Chair',  'OC-007', 349.00,  4, 2);

INSERT INTO orders (customer_id, status, total_amount) VALUES
    (1, 'shipped', 1329.98),
    (2, 'pending', 349.00);

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 1299.99),
    (1, 2, 1, 29.99),
    (2, 3, 1, 349.00);

INSERT INTO invoices (order_id, amount, status, due_date) VALUES
    (1, 1329.98, 'paid',   NOW() + INTERVAL '30 days'),
    (2, 349.00,  'unpaid', NOW() + INTERVAL '30 days');
