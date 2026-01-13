-- validation_queries.sql
-- Purpose: Validate data quality and integrity

-- 1. Row count validation
SELECT 'customer' as table_name, COUNT(*) as row_count FROM customer
UNION ALL
SELECT 'order', COUNT(*) FROM "order"
UNION ALL
SELECT 'product', COUNT(*) FROM product
UNION ALL
SELECT 'location', COUNT(*) FROM location
UNION ALL
SELECT 'sale_fact', COUNT(*) FROM sale_fact;

-- 2. Check for orphaned FK records
-- Customer FK check
SELECT COUNT(*) as orphaned_customers
FROM sale_fact sf
LEFT JOIN customer c ON sf.Customer_ID = c.Customer_ID
WHERE c.Customer_ID IS NULL;

-- Order FK check
SELECT COUNT(*) as orphaned_orders
FROM sale_fact sf
LEFT JOIN "order" o ON sf.Order_ID = o.Order_ID
WHERE o.Order_ID IS NULL;

-- Product FK check
SELECT COUNT(*) as orphaned_products
FROM sale_fact sf
LEFT JOIN product p ON sf.Product_ID = p.Product_ID
WHERE p.Product_ID IS NULL;

-- Location FK check
SELECT COUNT(*) as orphaned_locations
FROM sale_fact sf
LEFT JOIN location l ON sf.Location_Id = l.Location_Id
WHERE l.Location_Id IS NULL;

-- 3. Business logic validation
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN Sales <= 0 THEN 1 ELSE 0 END) as invalid_sales,
    SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) as invalid_quantity,
    SUM(CASE WHEN Discount < 0 OR Discount > 1 THEN 1 ELSE 0 END) as invalid_discount
FROM sale_fact;

-- 4. Date validation (Order_Date should be <= Ship_Date)
SELECT COUNT(*) as invalid_dates
FROM sale_fact sf
JOIN "order" o ON sf.Order_ID = o.Order_ID
WHERE o.Order_Date > o.Ship_Date;

