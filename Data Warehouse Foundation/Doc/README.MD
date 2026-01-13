# ETL Process Documentation
**Data Warehouse Foundation**

---

## Overview

**Pipeline:** `Kaggle (CSV) → Excel (Transform) → PostgreSQL (Load)`

| Metric | Value |
|--------|-------|
| Source | Kaggle Superstore Dataset |
| Target | PostgreSQL Star Schema (3NF) |
| Total Rows | 9,994 (fact table) |
| ETL Method | Manual (Excel + SQL COPY) |

---

## 1. Extract

**Source:** Kaggle Superstore Dataset  
**Method:** Manual download via browser  
**Format:** Single CSV file (~10K rows, 20 columns)  
**File:** `superstore.csv`

---

## 2. Transform (Microsoft Excel)

### Data Cleaning
- **Duplicates:** Removed using "Remove Duplicates" feature
- **Data Types:** Converted Order_Date, Ship_Date to DATE format, Sales/Profit to DECIMAL
- **Validation:** 
  - Sales > 0
  - Order_Date ≤ Ship_Date
  - Discount between 0-1

### Feature Engineering
- **New Column:** `Unit_Price = Sales / Quantity`

### Normalization (1 Table → 5 Tables)

**Original:** 1 denormalized table with repeated data  
**Target:** Star Schema (1 Fact + 4 Dimensions)

#### Tables Created:

| Table | Rows | Method | Columns |
|-------|------|--------|---------|
| **customer** | 793 | Remove duplicates on Customer_ID | Customer_ID (PK), Customer_Name, Segment |
| **order** | 5,009 | Remove duplicates on Order_ID | Order_ID (PK), Order_Date, Ship_Date, Ship_Mode |
| **product** | 1,862 | Remove duplicates on Product_ID | Product_ID (PK), Product_Name, Category, Sub_Category |
| **location** | 631 | Remove duplicates + add surrogate key | Location_Id (PK), Country, State, City, Postal_Code, Region |
| **sale_fact** | 9,994 | Add Sale_Id, VLOOKUP Location_Id | Sale_Id (PK), Order_ID (FK), Product_ID (FK), Customer_ID (FK), Location_Id (FK), Sales, Quantity, Discount, Profit, Unit_Price |

**Key Transformations:**
- Generated `Location_Id` using `=ROW()-1`
- Mapped Location_Id to fact table using `VLOOKUP`
- Exported each table as separate CSV

---

## 3. Load (PostgreSQL)

### Method: `COPY` Command

**Load Order:** Dimensions first, then Fact table

```sql
-- 1. Load dimensions (no dependencies)
COPY customer FROM '/path/customer.csv' CSV HEADER;
COPY "order" FROM '/path/order.csv' CSV HEADER;
COPY product FROM '/path/product.csv' CSV HEADER;
COPY location FROM '/path/location.csv' CSV HEADER;

-- 2. Load fact table (has FK dependencies)
COPY sale_fact FROM '/path/sale_fact.csv' CSV HEADER;
```

**Performance:** ~5 seconds total for 18,289 rows

---

## 4. Validation

### Row Count Check
```sql
SELECT 'sale_fact' as table, COUNT(*) FROM sale_fact;  -- 9,994
```

### Referential Integrity
```sql
-- Check for orphaned FK records (should return 0)
SELECT COUNT(*) FROM sale_fact sf
LEFT JOIN customer c ON sf.Customer_ID = c.Customer_ID
WHERE c.Customer_ID IS NULL;  -- Expected: 0
```

### Business Logic
```sql
-- Verify no invalid data
SELECT 
    SUM(CASE WHEN Sales <= 0 THEN 1 ELSE 0 END) as invalid_sales,
    SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) as invalid_qty
FROM sale_fact;  -- Both should be 0
```

---

## Data Lineage

```
Kaggle CSV (9,994 rows)
    ↓
Excel Transformation
    ├── customer.csv (793)
    ├── order.csv (5,009)
    ├── product.csv (1,862)
    ├── location.csv (631)
    └── sale_fact.csv (9,994)
    ↓
PostgreSQL COPY
    ↓
Star Schema Database (5 tables)
```

---

## Key Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| No Location_Id in source | Generated surrogate key in Excel using `ROW()` |
| Mapping Location to Fact table | Used VLOOKUP to find Location_Id |
| Date format consistency | Formatted as YYYY-MM-DD before CSV export |
| Excel performance with formulas | Used manual calculation mode, paste values |

---

## Files Structure

```
/01_data_warehouse/
├── data/
│   ├── raw/superstore.csv
│   └── cleaned/
│       ├── customer.csv
│       ├── order.csv
│       ├── product.csv
│       ├── location.csv
│       └── sale_fact.csv
├── sql/
│   ├── schema.sql
│   ├── load_data.sql
│   └── validation_queries.sql
└── docs/
    └── ETL_Documentation.md
```

---

## Summary

✅ **ETL Complete**  
- 9,994 transactions loaded successfully
- Star Schema with 5 normalized tables
- All FK relationships validated
- Ready for Phase 2 (Analytics & Dashboard)

**Next:** Query data with Python/Pandas → Export CSV → Tableau visualization