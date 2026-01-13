-- load_data.sql
-- Purpose: Load data from CSV files into PostgreSQL

-- Load dimensions first (no FK dependencies)
COPY customer(Customer_ID, Customer_Name, Segment)
FROM '/path/to/data/cleaned/customer.csv'
DELIMITER ','
CSV HEADER;

COPY "order"(Order_ID, Order_Date, Ship_Date, Ship_Mode)
FROM '/path/to/data/cleaned/order.csv'
DELIMITER ','
CSV HEADER;

COPY product(Product_ID, Product_Name, Category, Sub_Category)
FROM '/path/to/data/cleaned/product.csv'
DELIMITER ','
CSV HEADER;

COPY location(Location_Id, Country, State, City, Postal_Code, Region)
FROM '/path/to/data/cleaned/location.csv'
DELIMITER ','
CSV HEADER;

-- Load fact table last (has FK dependencies)
COPY sale_fact(Sale_Id, Order_ID, Product_ID, Customer_ID, Location_Id, 
               Sales, Unit_Sale, Quantity, Discount, Profit)
FROM '/path/to/data/cleaned/sale_fact.csv'
DELIMITER ','
CSV HEADER;