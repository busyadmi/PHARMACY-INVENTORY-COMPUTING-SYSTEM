"""
Pharmacy Inventory Computing System - Phase 3: AI Demand Forecaster & Anomaly Detector
Features:
 - Time-Series Demand Forecasting (Moving Average & Exponential Smoothing)
 - Statistical Anomaly Detection (Sales Spikes & Inventory Variance Signals)
 - Automated Safety Stock & Reorder Point Calculation (s, Q Policy)
 - FEFO Waste Reduction Optimizer
"""

import sqlite3
import os
import math
from datetime import datetime, timedelta
import random

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "pharmacy.db")

def init_db_if_needed():
    """Initializes the database schema if not present."""
    schema_file = os.path.join(BASE_DIR, "pharmacy-schema.sql")
    if not os.path.exists(schema_file):
        schema_file = os.path.join(BASE_DIR, "pharmacy_schema.sql")

    if os.path.exists(schema_file):
        conn = sqlite3.connect(DB_PATH)
        with open(schema_file, "r") as f:
            schema_sql = f.read()
        conn.executescript(schema_sql)
        conn.commit()
        conn.close()

def generate_mock_sales_history():
    """Generates 90 days of historical sales data into pharmacy.db if empty."""
    init_db_if_needed()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if sales exist
    cursor.execute("SELECT COUNT(*) FROM sales")
    count = cursor.fetchone()[0]
    
    if count < 5:
        print("[AI Engine] Populating historical sales data for 90-day time-series analysis...")
        today = datetime.now()
        
        # Products: 1=Amoxicillin, 2=Paracetamol, 3=Metformin, 4=Ciprofloxacin, 5=Lipitor
        products = [
            (1, 1, 15.50, 5, 25),   # (product_id, batch_id, price, min_qty, max_qty)
            (2, 2, 5.00, 10, 50),
            (3, 3, 22.00, 2, 15),
            (4, 4, 30.00, 1, 10),
            (5, 5, 45.00, 3, 20)
        ]
        
        for day_offset in range(90, 0, -1):
            sale_date = (today - timedelta(days=day_offset)).strftime("%Y-%m-%d %H:%M:%S")
            
            # Simulate daily transactions across 3 counters
            for counter_id in [1, 2, 3]:
                if random.random() > 0.3:  # 70% chance of sale per counter per day
                    p = random.choice(products)
                    qty = random.randint(p[3], p[4])
                    
                    # Inject an artificial anomaly spike every ~25 days
                    if day_offset in [15, 45, 75] and p[0] == 1:
                        qty = 150  # Anomaly demand spike!
                        
                    total = qty * p[2]
                    
                    cursor.execute(
                        "INSERT INTO sales (counter_id, total_amount, created_at) VALUES (?, ?, ?)",
                        (counter_id, total, sale_date)
                    )
                    sale_id = cursor.lastrowid
                    
                    cursor.execute(
                        "INSERT INTO sale_items (sale_id, product_id, batch_id, quantity, unit_price, subtotal) VALUES (?, ?, ?, ?, ?, ?)",
                        (sale_id, p[0], p[1], qty, p[2], total)
                    )
        
        conn.commit()
        print("[AI Engine] 90-day historical dataset generated successfully.")
    conn.close()


def forecast_demand_and_reorder_point():
    """
    Computes 30-day demand forecast using Exponential Smoothing 
    and calculates safety stock + Reorder Point (s, Q).
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Query daily aggregate demand per product over the last 30 days
    query = """
        SELECT p.product_id, p.name, p.reorder_level, DATE(s.created_at) as sale_day, SUM(si.quantity) as daily_qty
        FROM sale_items si
        JOIN sales s ON si.sale_id = s.sale_id
        JOIN products p ON si.product_id = p.product_id
        WHERE s.created_at >= DATE('now', '-30 days')
        GROUP BY p.product_id, sale_day
        ORDER BY p.product_id, sale_day ASC
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    # Organize time series by product
    product_series = {}
    product_names = {}
    for r in rows:
        pid = r["product_id"]
        product_names[pid] = r["name"]
        if pid not in product_series:
            product_series[pid] = []
        product_series[pid].append(r["daily_qty"])
        
    forecasts = []
    alpha = 0.3  # Smoothing factor
    lead_time_days = 7  # Supplier lead time
    
    print("\n=== AI DEMAND FORECAST & REORDER POINT (s, Q) ANALYSIS ===")
    for pid, sales in product_series.items():
        if not sales:
            continue
            
        # Exponential smoothing time-series forecast
        smoothed = sales[0]
        for val in sales[1:]:
            smoothed = alpha * val + (1 - alpha) * smoothed
            
        daily_forecast = smoothed
        monthly_forecast = daily_forecast * 30
        
        # Calculate standard deviation for Safety Stock
        avg_sales = sum(sales) / len(sales)
        variance = sum((x - avg_sales) ** 2 for x in sales) / len(sales)
        std_dev = math.sqrt(variance)
        
        # Safety Stock = Z * std_dev * sqrt(lead_time)  [Z = 1.65 for 95% service level]
        safety_stock = math.ceil(1.65 * std_dev * math.sqrt(lead_time_days))
        reorder_point = math.ceil((daily_forecast * lead_time_days) + safety_stock)
        
        forecasts.append({
            "product_id": pid,
            "name": product_names[pid],
            "daily_avg_demand": round(daily_forecast, 2),
            "projected_30day_demand": math.ceil(monthly_forecast),
            "safety_stock": safety_stock,
            "reorder_point_s": reorder_point
        })
        
        print(f" -> {product_names[pid]}:")
        print(f"    Avg Daily Demand: {round(daily_forecast, 1)} units/day | Projected 30-Day: {math.ceil(monthly_forecast)} units")
        print(f"    Safety Stock: {safety_stock} units | Reorder Point (s): {reorder_point} units")
        
    return forecasts


def detect_sales_anomalies():
    """
    Statistical Anomaly Detection using Z-Score thresholding (|Z| > 2.5)
    to flag sudden sales spikes or potential theft/inventory mismatches.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = """
        SELECT s.sale_id, p.name, si.quantity, s.created_at, s.counter_id
        FROM sale_items si
        JOIN sales s ON si.sale_id = s.sale_id
        JOIN products p ON si.product_id = p.product_id
        ORDER BY s.created_at DESC
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return
        
    quantities = [r["quantity"] for r in rows]
    mean_q = sum(quantities) / len(quantities)
    variance = sum((x - mean_q) ** 2 for x in quantities) / len(quantities)
    std_q = math.sqrt(variance) if variance > 0 else 1
    
    anomalies = []
    for r in rows:
        z_score = (r["quantity"] - mean_q) / std_q
        if abs(z_score) > 2.5:  # Anomaly signal threshold
            anomalies.append({
                "sale_id": r["sale_id"],
                "product_name": r["name"],
                "quantity": r["quantity"],
                "z_score": round(z_score, 2),
                "timestamp": r["created_at"],
                "counter_id": r["counter_id"]
            })
            
    print("\n=== ANOMALY SIGNAL DETECTION (Isolation / Z-Score Analysis) ===")
    if anomalies:
        print(f"[Alert] Found {len(anomalies)} Anomaly Signal Spikes (|Z| > 2.5):")
        for a in anomalies[:5]:  # Display top 5
            print(f" ⚠️  [Sale #{a['sale_id']}] Counter #{a['counter_id']} | Product: {a['product_name']} | Qty: {a['quantity']} (Z-Score: {a['z_score']}) at {a['timestamp']}")
    else:
        print("[Status] No statistical sales anomalies detected in recent transaction streams.")


if __name__ == "__main__":
    print("=== PHARMACY AI DEMAND FORECASTER & ANOMALY DETECTOR ===")
    generate_mock_sales_history()
    forecast_demand_and_reorder_point()
    detect_sales_anomalies()
