-- Pharmacy Inventory Computing System - Master Database Schema (SQL)
-- Architecture: Designed for Shared-Nothing & Multi-Threaded Parallel Query Processing
-- Supports GTIN tracking, FEFO (First-Expired, First-Out) batch control, and partitioned indexing.

-- 1. PRODUCTS TABLE (Master SKU Catalog)
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    gtin VARCHAR(14) UNIQUE NOT NULL,           -- Global Trade Item Number (GS1 Standard)
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    dosage_form VARCHAR(50),                     -- e.g., Tablet, Capsule, Syrup
    strength VARCHAR(50),                        -- e.g., 500mg, 10ml
    reorder_level INTEGER DEFAULT 50,            -- Safety Stock Trigger (s, Q policy)
    reorder_quantity INTEGER DEFAULT 200,        -- Economic Order Quantity
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexing GTIN for O(1) parallel hash lookup during POS scans
CREATE INDEX IF NOT EXISTS idx_products_gtin ON products(gtin);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

-- 2. BATCHES TABLE (FEFO Inventory & Expiry Tracking)
-- Partitioned logically by product_id to enable intra-query range/hash parallelism
CREATE TABLE IF NOT EXISTS batches (
    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    batch_number VARCHAR(100) NOT NULL,
    expiry_date DATE NOT NULL,                   -- FEFO sorting key
    quantity_in_stock INTEGER NOT NULL CHECK(quantity_in_stock >= 0),
    cost_price DECIMAL(10, 2) NOT NULL,
    received_date DATE DEFAULT CURRENT_DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

-- Indexing for fast parallel FEFO range scans and expiry alert filters
CREATE INDEX IF NOT EXISTS idx_batches_product_fefo ON batches(product_id, expiry_date ASC);
CREATE INDEX IF NOT EXISTS idx_batches_expiry ON batches(expiry_date ASC);

-- 3. SALES TRANSACTIONS TABLE (POS Checkout Counters)
CREATE TABLE IF NOT EXISTS sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    counter_id INTEGER NOT NULL,                 -- Multi-counter billing node ID
    total_amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50) DEFAULT 'CASH',   -- CASH, CARD, INSURANCE
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sales_counter_time ON sales(counter_id, created_at DESC);

-- 4. SALE ITEMS TABLE (Detailed Line Items)
CREATE TABLE IF NOT EXISTS sale_items (
    sale_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    batch_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (sale_id) REFERENCES sales(sale_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE INDEX IF NOT EXISTS idx_sale_items_product ON sale_items(product_id);

-- 5. PURCHASES TABLE (Supplier Replenishment Orders)
CREATE TABLE IF NOT EXISTS purchases (
    purchase_id INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name VARCHAR(255) NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE,
    total_cost DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'COMPLETED'       -- PENDING, COMPLETED, CANCELLED
);

-- 6. EXPENSES TABLE (Operating Costs Analysis)
CREATE TABLE IF NOT EXISTS expenses (
    expense_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category VARCHAR(100) NOT NULL,              -- Rent, Utilities, Logistics, Salaries
    amount DECIMAL(10, 2) NOT NULL,
    description TEXT,
    expense_date DATE DEFAULT CURRENT_DATE
);

-- 7. REORDER ALERTS TABLE (Automated Supply Chain Triggers)
CREATE TABLE IF NOT EXISTS reorder_alerts (
    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    current_stock INTEGER NOT NULL,
    reorder_level INTEGER NOT NULL,
    status VARCHAR(50) DEFAULT 'OPEN',          -- OPEN, ORDERED, RESOLVED
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- PRE-POPULATE DUMMY DATA FOR INITIALIZATION & TESTING
INSERT OR IGNORE INTO products (product_id, gtin, name, category, dosage_form, strength, reorder_level, reorder_quantity, unit_price) VALUES
(1, '00312345678906', 'Amoxicillin', 'Antibiotics', 'Capsule', '500mg', 50, 200, 15.50),
(2, '00398765432109', 'Paracetamol', 'Analgesics', 'Tablet', '650mg', 100, 500, 5.00),
(3, '00311122233344', 'Metformin', 'Antidiabetic', 'Tablet', '850mg', 30, 150, 22.00),
(4, '00355566677788', 'Ciprofloxacin', 'Antibiotics', 'Tablet', '500mg', 20, 100, 30.00),
(5, '00399988877766', 'Lipitor', 'Statins', 'Tablet', '20mg', 40, 150, 45.00);

INSERT OR IGNORE INTO batches (batch_id, product_id, batch_number, expiry_date, quantity_in_stock, cost_price) VALUES
(1, 1, 'AMX-2026-01', '2026-10-25', 120, 10.00),
(2, 2, 'PCM-2027-09', '2027-05-12', 450, 2.50),
(3, 3, 'MTF-2026-03', '2026-10-15', 25, 14.00),
(4, 4, 'CPR-2026-10', '2026-10-05', 12, 18.00),
(5, 5, 'LPT-2028-04', '2028-01-20', 200, 30.00);
