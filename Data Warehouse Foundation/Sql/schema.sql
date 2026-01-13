-- schema.sql
-- Purpose: Create all tables in the database

-- Create customer dimension
CREATE TABLE customer (
    Customer_ID VARCHAR(50) PRIMARY KEY,
    Customer_Name VARCHAR(255),
    Segment VARCHAR(50)
);

-- Create order dimension
CREATE TABLE "order" (
    Order_ID VARCHAR(50) PRIMARY KEY,
    Order_Date DATE,
    Ship_Date DATE,
    Ship_Mode VARCHAR(50)
);

-- Create product dimension
CREATE TABLE product (
    Product_ID VARCHAR(50) PRIMARY KEY,
    Product_Name VARCHAR(255),
    Category VARCHAR(50),
    Sub_Category VARCHAR(50)
);

-- Create location dimension
CREATE TABLE location (
    Location_Id INT PRIMARY KEY,
    Country VARCHAR(50),
    State VARCHAR(50),
    City VARCHAR(100),
    Postal_Code VARCHAR(20),
    Region VARCHAR(50)
);

-- Create fact table
CREATE TABLE sale_fact (
    Sale_Id INT PRIMARY KEY,
    Order_ID VARCHAR(50) REFERENCES "order"(Order_ID),
    Product_ID VARCHAR(50) REFERENCES product(Product_ID),
    Customer_ID VARCHAR(50) REFERENCES customer(Customer_ID),
    Location_Id INT REFERENCES location(Location_Id),
    Sales DECIMAL(10,2),
	Unit_Sale DECIMAL(10,2),
    Quantity INT,
    Discount DECIMAL(5,2),
    Profit DECIMAL(10,2)
);