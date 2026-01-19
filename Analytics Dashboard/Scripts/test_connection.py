"""
Superstore Analytics - Data Mart Export Script (psycopg2 version)
Purpose: Query PostgreSQL and export analytical datasets using pure psycopg2
No SQLAlchemy dependency required!
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime

# ==============================================
# CONFIGURATION
# ==============================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'Superstore',
    'user': 'postgres',
    'password': 'postgres'
}

# Output directory for CSV files
OUTPUT_DIR = '../Data Marts/'

# Create output directory if not exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==============================================
# DATABASE CONNECTION (psycopg2 only)
# ==============================================
def get_connection():
    """Create psycopg2 connection"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        return conn
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def execute_query(query, query_name):
    """Execute query and return DataFrame using psycopg2"""
    conn = None
    try:
        # Get connection
        conn = get_connection()
        if conn is None:
            return None
        
        print(f"⏳ Executing: {query_name}...")
        
        # Use pandas read_sql with psycopg2 connection
        df = pd.read_sql(query, conn)
        
        print(f"✅ {query_name}: {len(df)} rows retrieved")
        return df
        
    except Exception as e:
        print(f"❌ Error in {query_name}: {e}")
        return None
    finally:
        if conn:
            conn.close()

# ==============================================
# ANALYTICAL QUERIES (Same as before)
# ==============================================

# Query 1: Sales & Profit Trend (Monthly)
QUERY_SALES_TREND = """
SELECT 
    DATE_TRUNC('month', o.Order_Date) as Month,
    COUNT(DISTINCT sf.Order_ID) as Order_Count,
    COUNT(sf.Sale_Id) as Transaction_Count,
    SUM(sf.Sales) as Total_Sales,
    SUM(sf.Profit) as Total_Profit,
    ROUND(AVG(sf.Sales), 2) as Avg_Transaction_Value,
    ROUND(SUM(sf.Profit) / SUM(sf.Sales) * 100, 2) as Profit_Margin_Pct
FROM fact_sales sf
JOIN "orders" o ON sf.Order_ID = o.Order_ID
GROUP BY DATE_TRUNC('month', o.Order_Date)
ORDER BY Month
"""

# Query 2: Top 10 Products by Profit
QUERY_TOP_PRODUCTS = """
SELECT 
    p.Product_ID,
    p.Product_Name,
    p.Category,
    p.Sub_Category,
    COUNT(sf.Sale_Id) as Transaction_Count,
    SUM(sf.Quantity) as Units_Sold,
    SUM(sf.Sales) as Total_Sales,
    SUM(sf.Profit) as Total_Profit,
    ROUND(AVG(sf.Discount) * 100, 2) as Avg_Discount_Pct,
    ROUND(SUM(sf.Profit) / SUM(sf.Sales) * 100, 2) as Profit_Margin_Pct
FROM fact_sales sf
JOIN products p ON sf.Product_ID = p.Product_ID
GROUP BY p.Product_ID, p.Product_Name, p.Category, p.Sub_Category
ORDER BY Total_Profit DESC
LIMIT 10
"""

# Query 3: Sales by Region (Geographic Analysis)
QUERY_SALES_BY_REGION = """
SELECT 
    l.Region,
    l.Country,
    l.State,
    COUNT(DISTINCT sf.Customer_ID) as Customer_Count,
    COUNT(sf.Sale_Id) as Transaction_Count,
    SUM(sf.Sales) as Total_Sales,
    SUM(sf.Profit) as Total_Profit,
    ROUND(AVG(sf.Sales), 2) as Avg_Transaction_Value,
    ROUND(SUM(sf.Profit) / SUM(sf.Sales) * 100, 2) as Profit_Margin_Pct
FROM fact_sales sf
JOIN locations l ON sf.Location_Id = l.Location_Id
GROUP BY l.Region, l.Country, l.State
ORDER BY Total_Profit DESC
"""

# Query 4: Category Performance
QUERY_CATEGORY_PERFORMANCE = """
SELECT 
    p.Category,
    p.Sub_Category,
    COUNT(sf.Sale_Id) as Transaction_Count,
    SUM(sf.Quantity) as Units_Sold,
    SUM(sf.Sales) as Total_Sales,
    SUM(sf.Profit) as Total_Profit,
    ROUND(AVG(sf.Discount) * 100, 2) as Avg_Discount_Pct,
    ROUND(SUM(sf.Profit) / SUM(sf.Sales) * 100, 2) as Profit_Margin_Pct,
    COUNT(CASE WHEN sf.Profit < 0 THEN 1 END) as Loss_Transactions,
    ROUND(COUNT(CASE WHEN sf.Profit < 0 THEN 1 END) * 100.0 / COUNT(*), 2) as Loss_Rate_Pct
FROM fact_sales sf
JOIN products p ON sf.Product_ID = p.Product_ID
GROUP BY p.Category, p.Sub_Category
ORDER BY Total_Profit DESC
"""

# Query 5: Customer Segments Analysis
QUERY_CUSTOMER_SEGMENTS = """
SELECT 
    c.Segment,
    COUNT(DISTINCT c.Customer_ID) as Customer_Count,
    COUNT(sf.Sale_Id) as Transaction_Count,
    SUM(sf.Sales) as Total_Sales,
    SUM(sf.Profit) as Total_Profit,
    ROUND(AVG(sf.Sales), 2) as Avg_Transaction_Value,
    ROUND(SUM(sf.Sales) / COUNT(DISTINCT c.Customer_ID), 2) as Avg_Revenue_Per_Customer,
    ROUND(SUM(sf.Profit) / SUM(sf.Sales) * 100, 2) as Profit_Margin_Pct
FROM fact_sales sf
JOIN customers c ON sf.Customer_ID = c.Customer_ID
GROUP BY c.Segment
ORDER BY Total_Profit DESC
"""

# Query 6: Discount vs Profit Analysis
QUERY_DISCOUNT_ANALYSIS = """
SELECT 
    CASE 
        WHEN Discount = 0 THEN '0% (No Discount)'
        WHEN Discount <= 0.2 THEN '01-20%'
        WHEN Discount <= 0.4 THEN '21-40%'
        WHEN Discount <= 0.6 THEN '41-60%'
        ELSE '61-100%'
    END as Discount_Range,
    COUNT(*) as Transaction_Count,
    SUM(Sales) as Total_Sales,
    SUM(Profit) as Total_Profit,
    COUNT(CASE WHEN Profit < 0 THEN 1 END) as Loss_Count,
    ROUND(COUNT(CASE WHEN Profit < 0 THEN 1 END) * 100.0 / COUNT(*), 2) as Loss_Rate_Pct,
    ROUND(AVG(Sales), 2) as Avg_Sales,
    ROUND(AVG(Profit), 2) as Avg_Profit,
    ROUND(SUM(Profit) / SUM(Sales) * 100, 2) as Profit_Margin_Pct
FROM fact_sales
GROUP BY 
    CASE 
        WHEN Discount = 0 THEN '0% (No Discount)'
        WHEN Discount <= 0.2 THEN '01-20%'
        WHEN Discount <= 0.4 THEN '21-40%'
        WHEN Discount <= 0.6 THEN '41-60%'
        ELSE '61-100%'
    END
ORDER BY Discount_Range
"""

# Query 7: Top 10 Products with LOSSES
QUERY_TOP_LOSSES = """
SELECT 
    p.Product_ID,
    p.Product_Name,
    p.Category,
    p.Sub_Category,
    COUNT(sf.Sale_Id) as Transaction_Count,
    SUM(sf.Quantity) as Units_Sold,
    SUM(sf.Sales) as Total_Sales,
    SUM(sf.Profit) as Total_Loss,
    ROUND(AVG(sf.Discount) * 100, 2) as Avg_Discount_Pct,
    ROUND(SUM(sf.Profit) / SUM(sf.Sales) * 100, 2) as Loss_Margin_Pct
FROM fact_sales sf
JOIN products p ON sf.Product_ID = p.Product_ID
GROUP BY p.Product_ID, p.Product_Name, p.Category, p.Sub_Category
HAVING SUM(sf.Profit) < 0
ORDER BY Total_Loss ASC
LIMIT 10
"""

# Query 8: Profit vs Loss Overview
QUERY_PROFIT_LOSS_OVERVIEW = """
SELECT 
    'Overall Summary' as Metric_Type,
    COUNT(*) as Total_Transactions,
    COUNT(CASE WHEN Profit > 0 THEN 1 END) as Profitable_Transactions,
    COUNT(CASE WHEN Profit < 0 THEN 1 END) as Loss_Transactions,
    COUNT(CASE WHEN Profit = 0 THEN 1 END) as Break_Even_Transactions,
    SUM(Sales) as Total_Sales,
    SUM(CASE WHEN Profit > 0 THEN Profit END) as Total_Profit,
    SUM(CASE WHEN Profit < 0 THEN Profit END) as Total_Loss,
    SUM(Profit) as Net_Profit,
    ROUND(COUNT(CASE WHEN Profit < 0 THEN 1 END) * 100.0 / COUNT(*), 2) as Loss_Transaction_Pct,
    ROUND(SUM(CASE WHEN Profit < 0 THEN Profit END) / SUM(Profit) * 100, 2) as Loss_Impact_Pct
FROM fact_sales

UNION ALL

SELECT 
    p.Category as Metric_Type,
    COUNT(*) as Total_Transactions,
    COUNT(CASE WHEN sf.Profit > 0 THEN 1 END) as Profitable_Transactions,
    COUNT(CASE WHEN sf.Profit < 0 THEN 1 END) as Loss_Transactions,
    COUNT(CASE WHEN sf.Profit = 0 THEN 1 END) as Break_Even_Transactions,
    SUM(sf.Sales) as Total_Sales,
    SUM(CASE WHEN sf.Profit > 0 THEN sf.Profit END) as Total_Profit,
    SUM(CASE WHEN sf.Profit < 0 THEN sf.Profit END) as Total_Loss,
    SUM(sf.Profit) as Net_Profit,
    ROUND(COUNT(CASE WHEN sf.Profit < 0 THEN 1 END) * 100.0 / COUNT(*), 2) as Loss_Transaction_Pct,
    ROUND(SUM(CASE WHEN sf.Profit < 0 THEN sf.Profit END) / SUM(sf.Profit) * 100, 2) as Loss_Impact_Pct
FROM fact_sales sf
JOIN products p ON sf.Product_ID = p.Product_ID
GROUP BY p.Category

ORDER BY Metric_Type
"""

# ==============================================
# EXPORT FUNCTIONS
# ==============================================
def export_to_csv(df, filename):
    """Export DataFrame to CSV"""
    if df is not None and not df.empty:
        filepath = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(filepath, index=False)
        print(f"💾 Exported: {filepath}")
        return True
    else:
        print(f"⚠️  No data to export for {filename}")
        return False

def create_data_marts():
    """Execute all queries and export Data Marts"""
    
    print("\n" + "=" * 60)
    print("SUPERSTORE ANALYTICS - DATA MART EXPORT (psycopg2)")
    print("=" * 60 + "\n")
    
    start_time = datetime.now()
    
    # Test connection first
    print("Testing database connection...")
    conn = get_connection()
    if conn is None:
        print("❌ Cannot connect to database. Please check DB_CONFIG.")
        return {}
    else:
        print("✅ Database connection successful!\n")
        conn.close()
    
    # Dictionary of queries
    queries = {
        'sales_trend.csv': QUERY_SALES_TREND,
        'top_products.csv': QUERY_TOP_PRODUCTS,
        'sales_by_region.csv': QUERY_SALES_BY_REGION,
        'category_performance.csv': QUERY_CATEGORY_PERFORMANCE,
        'customer_segments.csv': QUERY_CUSTOMER_SEGMENTS,
        'discount_analysis.csv': QUERY_DISCOUNT_ANALYSIS,
        'top_losses.csv': QUERY_TOP_LOSSES,
        'profit_loss_overview.csv': QUERY_PROFIT_LOSS_OVERVIEW
    }
    
    # Execute and export
    results = {}
    for filename, query in queries.items():
        query_name = filename.replace('.csv', '').replace('_', ' ').title()
        df = execute_query(query, query_name)
        results[filename] = df
        export_to_csv(df, filename)
        print()  # Empty line for readability
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Summary
    print("=" * 60)
    print("EXPORT SUMMARY")
    print("=" * 60)
    print(f"✅ Total Data Marts Created: {len(queries)}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    print(f"⏱️  Total Time: {duration:.2f} seconds")
    print("=" * 60 + "\n")
    
    return results

# ==============================================
# DATA PREVIEW
# ==============================================
def preview_data_marts(results, preview_rows=5):
    """Preview first few rows of each data mart"""
    print("\n" + "=" * 60)
    print("DATA MART PREVIEW")
    print("=" * 60 + "\n")
    
    for filename, df in results.items():
        if df is not None and not df.empty:
            print(f"\n📊 {filename}")
            print("-" * 60)
            print(df.head(preview_rows).to_string(index=False))
            print()

# ==============================================
# MAIN EXECUTION
# ==============================================
if __name__ == "__main__":
    # Create all data marts
    results = create_data_marts()
    
    # Preview data (optional)
    preview_data_marts(results, preview_rows=3)
    
    print("✅ Data Mart export complete!")
    print(f"📂 Check output directory: {OUTPUT_DIR}")
    print("\n🚀 Ready for Tableau visualization!\n")