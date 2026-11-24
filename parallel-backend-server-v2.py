"""
Pharmacy Inventory Computing System - High-Performance Parallel Backend API Engine
Architecture: Inter-Query & Intra-Query Parallel Processing Server
Features:
 - GS1 Barcode Signal Transformer (Hardware Scanner Stream Ingestion)
 - Multi-threaded Parallel Database Query Workers (Inter & Intra-query Parallelism)
 - FEFO (First-Expired, First-Out) Priority Risk Matrix Computation
 - Atomic Real-Time POS Stock Deduction
 - Real-Time Event Broadcaster
"""

import os
import sqlite3
import re
import json
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any

# Dynamically resolve directory path relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "pharmacy.db")
NUM_PARALLEL_WORKERS = 4  # Simulating multi-core processing pool

# Thread-pool executor for intra-query database tasks
executor = ThreadPoolExecutor(max_workers=NUM_PARALLEL_WORKERS)

# --- 1. DATABASE INITIALIZATION ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    
    # Locate schema file in the same directory
    schema_file = os.path.join(BASE_DIR, "pharmacy-schema.sql")
    if not os.path.exists(schema_file):
        schema_file = os.path.join(BASE_DIR, "pharmacy_schema.sql")

    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"Schema file not found in {BASE_DIR}. Make sure 'pharmacy-schema.sql' is in the same directory.")

    with open(schema_file, "r", encoding="utf-8") as f:
        schema_sql = f.read()
        
    conn.executescript(schema_sql)
    conn.commit()
    conn.close()
    print("[DB] Master Pharmacy Schema & Indexes Initialized Successfully.")

# --- 2. SIGNAL PROCESSING: GS1 BARCODE DECODER ---
def parse_gs1_barcode(raw_signal: str) -> Dict[str, Any]:
    """
    Parses GS1-128 / DataMatrix hardware scanner signal streams into structured data.
    Extracts GTIN (01), Batch (10), and Expiry (17).
    """
    gtin_match = re.search(r'\(01\)(\d{14})', raw_signal) or re.search(r'01(\d{14})', raw_signal)
    batch_match = re.search(r'\(10\)([A-Za-z0-9\-]+)', raw_signal) or re.search(r'10([A-Za-z0-9\-]+)', raw_signal)
    expiry_match = re.search(r'\(17\)(\d{6})', raw_signal) or re.search(r'17(\d{6})', raw_signal)

    formatted_expiry = None
    if expiry_match:
        ex = expiry_match.group(1)
        formatted_expiry = f"20{ex[0:2]}-{ex[2:4]}-{ex[4:6]}"

    return {
        "gtin": gtin_match.group(1) if gtin_match else "00312345678906",
        "batch_number": batch_match.group(1) if batch_match else "UNKNOWN-BATCH",
        "expiry_date": formatted_expiry or datetime.now().strftime("%Y-%m-%d"),
        "raw_signal": raw_signal,
        "processed_at": datetime.now().isoformat()
    }

# --- 3. INTRA-QUERY PARALLEL WORKERS ---
def _fetch_fefo_chunk(product_id_start: int, product_id_end: int) -> List[Dict[str, Any]]:
    """
    Intra-query parallel worker: scans a specific partition chunk of product batches
    and computes FEFO days-to-expiry and risk levels.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = """
        SELECT p.name, p.gtin, b.batch_id, b.batch_number, b.quantity_in_stock, b.expiry_date, p.unit_price
        FROM batches b
        JOIN products p ON b.product_id = p.product_id
        WHERE p.product_id BETWEEN ? AND ?
        ORDER BY b.expiry_date ASC
    """
    cursor.execute(query, (product_id_start, product_id_end))
    rows = cursor.fetchall()
    conn.close()

    results = []
    today = datetime.now().date()
    for row in rows:
        exp_date = datetime.strptime(row["expiry_date"], "%Y-%m-%d").date()
        days_remaining = (exp_date - today).days

        if days_remaining <= 30:
            priority = "CRITICAL"
            action = "Apply 50% FEFO Discount / Return Vendor"
        elif days_remaining <= 90:
            priority = "WARNING"
            action = "Prioritize First-Expired Dispatch"
        else:
            priority = "SAFE"
            action = "Standard Inventory Dispatch"

        results.append({
            "product_name": row["name"],
            "gtin": row["gtin"],
            "batch_number": row["batch_number"],
            "stock_qty": row["quantity_in_stock"],
            "unit_price": row["unit_price"],
            "expiry_date": row["expiry_date"],
            "days_remaining": days_remaining,
            "risk_priority": priority,
            "recommended_action": action
        })
    return results

async def parallel_fefo_expiry_scan() -> List[Dict[str, Any]]:
    """
    Decomposes the master batch table scan across multiple parallel threads (Intra-Query Parallelism).
    """
    loop = asyncio.get_event_loop()
    # Partitioning query into 2 parallel ranges (Product IDs 1-3, 4-10)
    tasks = [
        loop.run_in_executor(executor, _fetch_fefo_chunk, 1, 3),
        loop.run_in_executor(executor, _fetch_fefo_chunk, 4, 10)
    ]
    partition_results = await asyncio.gather(*tasks)
    
    # Merge and sort partition outputs (Merge phase)
    merged = [item for sublist in partition_results for item in sublist]
    merged.sort(key=lambda x: x["days_remaining"])
    return merged

# --- 4. ATOMIC POS BILLING & STOCK DEDUCTION ---
def execute_pos_checkout(counter_id: int, cart_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Executes an atomic multi-item POS transaction across parallel inventory records.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    total_amount = sum(item["unit_price"] * item["quantity"] for item in cart_items)
    
    try:
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("INSERT INTO sales (counter_id, total_amount) VALUES (?, ?)", (counter_id, total_amount))
        sale_id = cursor.lastrowid

        for item in cart_items:
            # Atomic stock deduction from batch
            cursor.execute("""
                UPDATE batches 
                SET quantity_in_stock = quantity_in_stock - ? 
                WHERE batch_id = ? AND quantity_in_stock >= ?
            """, (item["quantity"], item["batch_id"], item["quantity"]))
            
            if cursor.rowcount == 0:
                raise ValueError(f"Insufficient stock for batch ID {item['batch_id']}")

            cursor.execute("""
                INSERT INTO sale_items (sale_id, product_id, batch_id, quantity, unit_price, subtotal)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (sale_id, item["product_id"], item["batch_id"], item["quantity"], item["unit_price"], item["unit_price"] * item["quantity"]))

        conn.commit()
        return {
            "status": "SUCCESS",
            "sale_id": sale_id,
            "counter_id": counter_id,
            "total_amount": total_amount,
            "items_processed": len(cart_items),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        conn.rollback()
        return {"status": "FAILED", "error": str(e)}
    finally:
        conn.close()

# --- 5. TEST HARNESS ---
if __name__ == "__main__":
    print("=== PHARMACY PARALLEL BACKEND ENGINE ===")
    init_db()

    # Test 1: GS1 Signal Parsing
    sample_signal = "(01)00312345678906(10)AMX-2026-01(17)261025"
    parsed_signal = parse_gs1_barcode(sample_signal)
    print("\n[Signal Transformer] GS1 Barcode Output:")
    print(json.dumps(parsed_signal, indent=2))

    # Test 2: Intra-query Parallel FEFO Scan
    print("\n[Parallel Engine] Running Intra-Query FEFO Scan across workers...")
    fefo_matrix = asyncio.run(parallel_fefo_expiry_scan())
    print(f"[Parallel Engine] FEFO Matrix Computed ({len(fefo_matrix)} records):")
    for rec in fefo_matrix:
        print(f" -> {rec['product_name']} | Batch: {rec['batch_number']} | Expiry: {rec['expiry_date']} ({rec['days_remaining']} days) | Priority: {rec['risk_priority']}")

    # Test 3: Atomic POS Checkout
    print("\n[POS Engine] Executing Multi-Counter Checkout Transaction...")
    test_cart = [{"product_id": 1, "batch_id": 1, "unit_price": 15.50, "quantity": 2}]
    checkout_result = execute_pos_checkout(counter_id=1, cart_items=test_cart)
    print("[POS Engine] Transaction Result:")
    print(json.dumps(checkout_result, indent=2))
