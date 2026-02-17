# Notebook Documentation — Synapse Dedicated SQL Pool DWH Demo (Bronze/Silver/Gold, SCD2, 20M Fact)

## Overview
This notebook provisions and loads a demo data warehouse in an **Azure Synapse Dedicated SQL pool**. It creates a three-layer structure (**Bronze/Silver/Gold**), generates synthetic data (including a **20M-row** sales fact), maintains **SCD Type 2** dimensions, refreshes **reporting aggregates**, and provides **workload queries** for scheduled execution.

Execution is orchestrated from a Synapse **Spark notebook**, while all DDL/DML is executed on the **Dedicated SQL pool** via JDBC.

---

## Prerequisites

### Required services
- Synapse Workspace with:
  - A **Spark pool** (to run the notebook)
  - A **Dedicated SQL pool** database (DW), e.g. `migrateMe`

### Required authentication (SQL Login)
The notebook connects via JDBC using **SQL authentication**, which requires:
1) a **server-level SQL login** created in the **master** database, and
2) a **database user** created in the **Dedicated SQL pool database** (e.g. `migrateMe`), mapped to that login.

---

## Create SQL login and database user (SSMS instructions)

### Step 1 — Connect to the Synapse SQL endpoint in SSMS
1. Open **SQL Server Management Studio (SSMS)**.
2. Connect to:
   - **Server name:** `<your-workspace>.sql.azuresynapse.net`  
     (example: `demosynapseazcomm.sql.azuresynapse.net`)
   - **Authentication:** use an admin login that can create logins (SQL admin / AAD admin as applicable).
3. After connecting, you should see:
   - `master` database
   - your Dedicated SQL pool database (e.g. `migrateMe`)

### Step 2 — Create the SQL Login in `master`
1. In SSMS, open a **New Query** window.
2. Set database context to `master` (dropdown in SSMS toolbar, or execute `USE master;`).
3. Run:

```sql
USE master;
GO

CREATE LOGIN [demo_etl_user]
WITH PASSWORD = 'Put-A-Strong-Password-Here!',
     CHECK_POLICY = ON,
     CHECK_EXPIRATION = OFF;
GO
```

### Step 3 — Create the database user in the Dedicated SQL pool database
1. In SSMS, switch the database context to your dedicated pool database, e.g. `migrateMe`.
2. Run:

```sql
USE migrateMe;
GO

CREATE USER [demo_etl_user] FOR LOGIN [demo_etl_user];
GO

-- Demo-friendly permissions:
EXEC sp_addrolemember 'db_owner', 'demo_etl_user';
GO
```

> Alternative: Instead of `db_owner`, grant a minimal set of permissions, but for demo setup `db_owner` is simplest.

### Step 4 — Use the credentials in the notebook
In the notebook configuration section, set:
- `SQL_USER = "demo_etl_user"`
- `SQL_PASSWORD = "Put-A-Strong-Password-Here!"`

---

## Configuration
The notebook contains a parameter section defining:
- Dedicated SQL endpoint and database name
- SQL authentication credentials used by JDBC
- Flags controlling whether the initial 20M load runs
- Delta volume for daily incremental loads

---

## Notebook Sections

### 1) JDBC Executor (PySpark)
**Purpose:** Provide reusable functions to execute T‑SQL statements on the Dedicated SQL pool.

**What it does:**
- Builds a JDBC connection string to the Dedicated SQL pool.
- Creates a connection using the SQL Server JDBC driver.
- Exposes helper functions:
  - `exec_sql(sql_text)` — executes one SQL batch.
  - `exec_batch([sql1, sql2, ...])` — executes a list of batches sequentially with progress output.

---

### 2) Connectivity Test
**Purpose:** Validate that the notebook can connect to the Dedicated SQL pool.

**What it does:**
- Executes a lightweight `SELECT` statement.
- Confirms the database context and engine identity (database name, login name, version).

---

### 3) One-Time Setup — Schemas (Bronze/Silver/Gold)
**Purpose:** Establish logical layers used by the warehouse.

**What it does:**
- Creates schemas:
  - `bronze` (raw landing)
  - `silver` (conformed / history-aware)
  - `gold` (reporting-ready)

---

### 4) One-Time Setup — `dbo.Numbers` (20,000,000 rows)
**Purpose:** Provide a deterministic row generator used to produce large synthetic datasets inside the Dedicated SQL pool.

**What it does:**
- Creates a seed table, inserts an initial rowset from system views, and expands by repeated doubling.
- Creates the final table `dbo.Numbers` using CTAS so it contains **exactly 20,000,000 rows**, numbered `1..20000000`.
- Drops the seed table after completion.

**Output:**
- `dbo.Numbers(n INT)` with `COUNT(*) = 20000000`.

---

### 5) One-Time Setup — Bronze Layer Tables
**Purpose:** Create raw (“landed”) structures.

**What it does:**
- Creates:
  - `bronze.CustomerRaw`
  - `bronze.ProductRaw`
  - `bronze.StoreRaw`
  - `bronze.SalesRaw`
- Tables are created as **HEAP** with `DISTRIBUTION = ROUND_ROBIN` to support fast ingest/staging.

---

### 6) One-Time Load — Bronze Dimensions
**Purpose:** Populate raw dimension-like entities.

**What it does:**
- Inserts synthetic data using `dbo.Numbers`:
  - Customers: 50,000
  - Products: 20,000
  - Stores: 400
- Uses deterministic `CASE` expressions to assign categories/segments/countries/channels/regions.

---

### 7) One-Time Load — Bronze Fact (20M Sales Lines)
**Purpose:** Populate the raw fact source for downstream processing.

**What it does:**
- Inserts **20,000,000** records into `bronze.SalesRaw` using `dbo.Numbers`.
- Generates:
  - `OrderDate` over a multi-year range
  - Randomized keys for Customer/Product/Store
  - Quantity, unit price, discount, and net amount
  - Extract timestamp and source system marker

---

### 8) One-Time Setup — Silver Layer (SCD2 Dimensions)
**Purpose:** Create conformed dimensions with history tracking.

**What it does:**
- Creates SCD Type 2 tables:
  - `silver.DimCustomer`
  - `silver.DimProduct`
  - `silver.DimStore`
- Each dimension includes:
  - Surrogate key (`…SK`, identity)
  - Natural key (`…NK`)
  - Attributes
  - `EffectiveFrom`, `EffectiveTo`
  - `IsCurrent`
- Performs an initial load from Bronze raw tables:
  - All rows are inserted as current (`IsCurrent = 1`, `EffectiveTo = '9999-12-31'`)

---

### 9) One-Time Setup — Silver Fact + Watermark
**Purpose:** Create analytics-optimized fact storage and enable incremental loads.

**What it does:**
- Creates `silver.FactSales` with:
  - Surrogate keys (`CustomerSK`, `ProductSK`, `StoreSK`)
  - Measures (qty, price, discount, net amount)
  - Load timestamp
- Uses `CLUSTERED COLUMNSTORE INDEX` for analytics performance.
- Creates `silver.ETL_Watermark` to track last loaded `SalesLineID`.
- Loads the initial fact by joining `bronze.SalesRaw` to dimensions using:
  - Natural key mapping
  - SCD2 effective dating (**as-of OrderDate**)

---

### 10) One-Time Setup — Gold Reporting Aggregate
**Purpose:** Provide a reporting-ready table for dashboards and BI tools.

**What it does:**
- Creates `gold.RptSalesMonthly` (columnstore) containing monthly aggregates:
  - Units, NetSales, ActiveCustomers
  - Grouped by YearMonth and common business cuts (channel, region, country, segment, category, subcategory)
- Populates from `silver.FactSales` joined to dimension attributes.

---

## Daily Batch Section (Rerunnable)

### 11) Bronze Delta Generator
**Purpose:** Simulate ongoing ingestion.

**What it does:**
- Appends `DELTA_ROWS_PER_RUN` new rows into `bronze.SalesRaw` with recent dates.
- Appends a small percentage of changed customer records into `bronze.CustomerRaw` to drive SCD2 history creation.

---

### 12) Apply SCD Type 2 (Customer)
**Purpose:** Maintain customer history in Silver.

**What it does:**
- Identifies latest customer records in Bronze per natural key.
- Detects attribute changes vs the current Silver row.
- Closes the current row (`IsCurrent = 0`, sets `EffectiveTo`).
- Inserts a new current row with updated attributes.

---

### 13) Incremental Fact Load (As-Of OrderDate)
**Purpose:** Load newly arrived sales records into the conformed fact.

**What it does:**
- Reads the last loaded `SalesLineID` from `silver.ETL_Watermark`.
- Inserts only new sales lines into `silver.FactSales`.
- Resolves dimension surrogate keys using SCD2 effective windows:
  - `OrderDate >= EffectiveFrom` and `OrderDate < EffectiveTo`
- Updates the watermark to the latest `SalesLineID`.

---

### 14) Gold Refresh
**Purpose:** Keep reporting aggregate current.

**What it does:**
- Truncates `gold.RptSalesMonthly`.
- Recomputes monthly aggregates from `silver.FactSales`.

---

## Workload Queries (For Scheduling)
The notebook includes representative queries intended for scheduled execution, such as:
- Recent-month KPI over the Gold aggregate
- 30-day sales by channel/region
- Top categories by country/segment over recent months
- 90-day drilldown grouped by multiple dimensions (heavier query)

---

## Outputs (Objects Created)
- Schemas: `bronze`, `silver`, `gold`
- Generator: `dbo.Numbers` (20M rows)
- Bronze: `CustomerRaw`, `ProductRaw`, `StoreRaw`, `SalesRaw`
- Silver: `DimCustomer`, `DimProduct`, `DimStore`, `FactSales`, `ETL_Watermark`
- Gold: `RptSalesMonthly`
