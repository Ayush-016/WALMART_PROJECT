# Walmart Data Engineering Project

An end-to-end **Walmart data engineering pipeline** built around **Apache Airflow, Databricks, dbt, Spark, Delta Lake and PostgreSQL**.

The project demonstrates how a source database can be ingested into a Databricks Bronze layer, transformed through Silver technical/business layers, modeled into Gold dimensions and facts, and orchestrated as a dependency-driven Airflow workflow.

> **Note:** The repository contains the Airflow/dbt orchestration and transformation code. The actual Databricks ingestion job is referenced by the Airflow DAG but its Databricks-side implementation is not included in this repository.

---

## Architecture

The project is designed around the following flow:

```text
                         ┌─────────────────────┐
                         │      Source Data    │
                         │   PostgreSQL / CSV  │
                         └──────────┬──────────┘
                                    │
                                    │ CDC / Incremental
                                    ▼
                         ┌─────────────────────┐
                         │      Databricks     │
                         │     Spark / Job     │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Bronze Layer     │
                         │ Delta Lake Tables   │
                         └──────────┬──────────┘
                                    │
                       ┌────────────▼────────────┐
                       │       dbt + Airflow     │
                       └────────────┬────────────┘
                                    │
                     ┌──────────────▼──────────────┐
                     │       Silver Technical      │
                     │   Incremental dbt Models    │
                     └──────────────┬──────────────┘
                                    │
                                    ▼
                     ┌─────────────────────────────┐
                     │       Silver Business       │
                     │       OBT / Wide Table      │
                     └──────────────┬──────────────┘
                                    │
                                    ▼
                     ┌─────────────────────────────┐
                     │        Gold Layer           │
                     │ Ephemeral + SCD Dimensions  │
                     │       + Fact Orders         │
                     └─────────────────────────────┘
```

The original architecture diagram is available in [`Notes.png`](Notes.png).

---

## What This Project Demonstrates

### Data ingestion

- PostgreSQL source data
- CSV-based sample dataset
- CDC/incremental ingestion trigger
- Databricks job orchestration through the Databricks SDK

### Data processing

- Apache Spark on Databricks
- Delta Lake Bronze tables
- Incremental dbt models
- `updated_timestamp` based incremental filtering

### Transformation

- dbt source definitions
- Silver technical models
- Silver business OBT (One Big Table)
- Jinja-driven SQL generation
- Gold ephemeral models
- dbt snapshots
- Fact modeling

### Data quality

- `not_null` tests
- `unique` tests
- Business-level custom SQL test
- Source freshness execution
- Airflow dependency control around dbt tests

### Orchestration

- Apache Airflow DAG
- Sequential task dependencies
- BashOperator/dbt execution
- Databricks job monitoring
- Dockerized Airflow environment

---

# Dataset

The repository contains six CSV datasets:

| Dataset | Rows |
|---|---:|
| `customers.csv` | 2,000 |
| `employees.csv` | 250 |
| `order_items.csv` | 30,021 |
| `orders.csv` | 10,000 |
| `products.csv` | 500 |
| `stores.csv` | 25 |

The data represents a simplified retail/Walmart-style relational model.

### Relationships

```text
customers
    │
    └──────────────┐
                   ▼
                orders
                   │
          ┌────────┴────────┐
          ▼                 ▼
    order_items           stores
          │                 │
          ▼                 ▼
       products         employees
```

### Source entities

#### Customers

- `customer_id`
- customer name/contact information
- location
- created/updated timestamps
- active flag

#### Stores

- `store_id`
- store name
- location
- created/updated timestamps
- active flag

#### Products

- `product_id`
- product name
- category
- brand
- price
- created/updated timestamps
- active flag

#### Employees

- `employee_id`
- `store_id`
- employee information
- job title
- salary
- created/updated timestamps
- active flag

#### Orders

- `order_id`
- `customer_id`
- `store_id`
- order timestamp
- payment method
- order status
- total amount
- created/updated timestamps
- active flag

#### Order Items

- `order_item_id`
- `order_id`
- `product_id`
- quantity
- unit price
- line amount
- created/updated timestamps
- active flag

---

# Airflow DAG

The main DAG is:

```text
orchestrate
```

Location:

```text
airflow_dbt_project/dags/orchestrate.py
```

The DAG executes the pipeline in the following order:

```text
ingest_cdc
     ↓
clean_target
     ↓
source_freshness
     ↓
silver_technical
     ↓
silver_technical_tests
     ↓
silver_business
     ↓
silver_business_tests
     ↓
gold_ephermeral
     ↓
gold_dimensions
     ↓
gold_facts
```

## 1. `ingest_cdc`

The DAG uses the Databricks SDK:

```python
WorkspaceClient(...)
```

to trigger an existing Databricks job.

The task then polls the Databricks job until it reaches a terminal state:

- `TERMINATED`
- `SKIPPED`
- `INTERNAL_ERROR`

A successful Databricks run allows the Airflow DAG to continue.

### Important

The repository currently contains placeholders for:

```text
your_databricks_host
your_databricks_token
your_databricks_job_id
```

These must be configured before running the DAG.

---

## 2. `clean_target`

Removes dbt-generated target/log directories:

```bash
rm -rf /opt/airflow/walmart_project/target
rm -rf /opt/airflow/walmart_project/logs
```

This gives the dbt execution a clean local target state.

---

## 3. `source_freshness`

Runs:

```bash
dbt source freshness
```

This checks the configured dbt sources before transformation begins.

Sources are defined in:

```text
walmart_project/models/source/sources.yml
```

The project expects the following Bronze tables:

```text
walmart.orders
walmart.customers
walmart.products
walmart.order_items
walmart.stores
walmart.employees
```

with the source schema configured as:

```text
bronze
```

---

# dbt Transformation Layers

The dbt project is located at:

```text
airflow_dbt_project/walmart_project/
```

The configured layers are:

```text
Bronze
   │
   ▼
Silver Technical
   │
   ▼
Silver Business
   │
   ▼
Gold
```

---

## Silver Technical Layer

Directory:

```text
models/silver_t/
```

Models:

```text
customers_t.sql
employees_t.sql
order_items_t.sql
orders_t.sql
products_t.sql
stores_t.sql
```

Each model is configured as an incremental model.

For example:

```sql
{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}
```

The initial load reads the complete source table.

On subsequent runs:

```sql
{% if is_incremental() %}
WHERE updated_timestamp >
    (
        SELECT COALESCE(
            MAX(updated_timestamp),
            '1900-01-01'
        )
        FROM {{ this }}
    )
{% endif %}
```

This allows the model to process records newer than the latest processed timestamp instead of rebuilding the complete table every time.

Each Silver Technical model also adds:

```sql
current_timestamp() AS processed_at
```

### Incremental keys

| Model | Unique Key |
|---|---|
| `customers_t` | `customer_id` |
| `employees_t` | `employee_id` |
| `order_items_t` | `order_item_id` |
| `orders_t` | `order_id` |
| `products_t` | `product_id` |
| `stores_t` | `store_id` |

---

# Silver Business Layer

Directory:

```text
models/silver_b/
```

Main model:

```text
obt_b.sql
```

`obt_b` creates a business-oriented **One Big Table (OBT)** by joining the Silver Technical models.

The model joins:

```text
orders
   │
   ├── customers
   ├── order_items
   │      └── products
   ├── stores
   └── employees
```

The SQL uses a Jinja configuration list to dynamically generate:

- source table references
- selected columns
- aliases
- join conditions

This reduces repetitive SQL when constructing the OBT.

---

# Gold Layer

The Gold layer contains:

```text
models/gold/
├── ephemeral/
└── fact/
```

## Gold Ephemeral Models

The project contains five ephemeral models:

```text
eph_customers
eph_employees
eph_orders
eph_products
eph_stores
```

They are derived from:

```text
obt_b
```

and use:

```sql
{{ ref('obt_b') }}
```

The models use `DISTINCT` and select entity-specific attributes before being consumed by the snapshot layer.

Because they are configured as **ephemeral**, dbt does not create standalone physical tables for them. Their SQL is instead incorporated into downstream models.

---

# SCD Type 2 Snapshots

The Gold layer uses dbt snapshots to maintain historical versions of entities.

Snapshots:

```text
dim_customers
dim_employees
dim_orders
dim_products
dim_stores
```

The snapshot strategy is:

```text
timestamp
```

with the relevant:

```text
*_updated_timestamp
```

column used as `updated_at`.

For example:

```yaml
unique_key: customer_id
strategy: timestamp
updated_at: customer_updated_timestamp
```

The configuration also uses:

```yaml
dbt_valid_to_current: "to_date('9999-12-31')"
```

so current records receive a far-future validity end date instead of `NULL`.

This provides an SCD Type 2 style history containing:

- business key
- previous versions
- current version
- validity start
- validity end
- dbt change metadata

---

# Gold Fact

The final fact model is:

```text
models/gold/fact/fact_orders.sql
```

Model:

```text
fact_orders
```

It is created from:

```text
obt_b
```

and contains the core order-item level measures and keys:

```text
order_id
order_item_id
product_id
store_id
employee_id
customer_id
total_amount
quantity
unit_price
line_amount
```

This provides an analytical fact structure suitable for downstream reporting.

---

# Data Quality

## Silver Technical Tests

The project defines dbt tests in:

```text
models/silver_t/properties.yml
```

Current tests include:

### Products

```text
product_id
    ├── not_null
    └── unique where price > 0
```

### Orders

```text
order_id
    ├── not_null
    └── unique
```

These tests are executed by Airflow with:

```bash
dbt test --select silver_t
```

---

## Silver Business Test

The project also contains:

```text
tests/test_obt.sql
```

The test checks for missing critical keys in the OBT:

```text
order_id
product_id
employee_id
store_id
order_item_id
customer_id
```

The test is configured with:

```sql
{{ config(severity='warn') }}
```

and is executed by:

```bash
dbt test --select silver_b
```

---

# Jinja Macro

The project includes:

```text
macros/custom_schema.sql
```

The macro:

```text
generate_schema_name
```

controls how dbt resolves custom schema names.

This allows models to explicitly use schemas such as:

```text
silver_t
silver_b
gold
```

instead of relying only on dbt's default schema naming behavior.

---

# dbt Configuration

The project is configured with:

```yaml
silver_t:
  +materialized: table
  +schema: silver_t

silver_b:
  +materialized: table
  +schema: silver_b

gold:
  +materialized: table
  +schema: gold

gold.ephemeral:
  +materialized: ephemeral
```

Individual Silver Technical models override the default materialization to use:

```text
incremental
```

---

# PostgreSQL Dataset Setup

The repository contains:

```text
walmart_dataset/
```

with:

```text
walmart_dataset/
├── data/
│   ├── customers.csv
│   ├── employees.csv
│   ├── order_items.csv
│   ├── orders.csv
│   ├── products.csv
│   └── stores.csv
│
├── ddl/
│   └── walmart_schema.sql
│
└── load_data.py
```

The DDL defines the six source entities.

`load_data.py` uses `psycopg2` and PostgreSQL `COPY` to load the CSV files.

The loader maps:

```text
customers.csv    → raw.customers
stores.csv       → raw.stores
products.csv     → raw.products
employees.csv    → raw.employees
orders.csv       → raw.orders
order_items.csv  → raw.order_items
```

Before using the loader, make sure the PostgreSQL connection string and target schema/table setup match your environment.

---

# Dockerized Airflow Environment

The Airflow environment is under:

```text
airflow_dbt_project/
```

Docker Compose provides the local Airflow stack.

### Main services

```text
Airflow API Server
Airflow Scheduler
Airflow DAG Processor
Airflow Worker
Airflow Triggerer
PostgreSQL
Redis
Airflow Init
```

The project uses:

```text
CeleryExecutor
```

with:

```text
PostgreSQL
```

as the Airflow metadata database/result backend and:

```text
Redis
```

as the Celery broker.

The Airflow API is exposed on:

```text
http://localhost:8080
```

Default credentials from the compose configuration are:

```text
Username: airflow
Password: airflow
```

Change these for any non-local deployment.

---

# Requirements

The custom Airflow image installs:

```text
Apache Airflow >= 3.2.2
dbt-core >= 1.11.11
dbt-databricks >= 1.12.1
apache-airflow-providers-standard >= 0.11.0
```

The Dockerfile starts from the Apache Airflow image and installs the project requirements.

---

# Setup

## 1. Clone the repository

```bash
git clone <your-repository-url>
cd Walmart_Airflow_DBT_Project
```

## 2. Configure Airflow

Move into the Airflow project:

```bash
cd airflow_dbt_project
```

Create/configure:

```text
.env
```

The repository's `.env` is intentionally empty.

At minimum, configure the Airflow values required by the Docker Compose file, including:

```text
AIRFLOW_UID
FERNET_KEY
_AIRFLOW_WWW_USER_USERNAME
_AIRFLOW_WWW_USER_PASSWORD
```

For Linux, `AIRFLOW_UID` should normally match your host user ID.

---

## 3. Configure Databricks

Update the placeholders in:

```text
dags/orchestrate.py
```

and:

```text
walmart_project/profiles.yml
```

with your Databricks:

```text
Host
Token
HTTP Path
Job ID
```

Do not commit real credentials or tokens to Git.

For production, use a secrets manager or Airflow connection/secret backend rather than hard-coding credentials.

---

## 4. Build and initialize Airflow

From:

```text
airflow_dbt_project/
```

run:

```bash
docker compose build
```

then:

```bash
docker compose up airflow-init
```

Start the services:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker compose ps
```

Open:

```text
http://localhost:8080
```

---

# Running the Pipeline

Once the Databricks job and dbt connection are configured:

1. Open Airflow.
2. Locate the `orchestrate` DAG.
3. Enable/unpause the DAG.
4. Trigger it manually or schedule it.
5. Monitor each task in order.

Expected sequence:

```text
ingest_cdc
      ↓
clean_target
      ↓
source_freshness
      ↓
silver_technical
      ↓
silver_technical_tests
      ↓
silver_business
      ↓
silver_business_tests
      ↓
gold_ephermeral
      ↓
gold_dimensions
      ↓
gold_facts
```

---

# Running dbt Manually

If Databricks is already configured and the dbt environment is available:

```bash
cd airflow_dbt_project/walmart_project
```

Check the connection:

```bash
dbt debug
```

Run source freshness:

```bash
dbt source freshness
```

Build Silver Technical:

```bash
dbt run --select silver_t
```

Test Silver Technical:

```bash
dbt test --select silver_t
```

Build Silver Business:

```bash
dbt run --select silver_b
```

Test Silver Business:

```bash
dbt test --select silver_b
```

Build Gold ephemeral models:

```bash
dbt run --select gold/ephermeral
```

Run snapshots:

```bash
dbt snapshot
```

Build Gold facts:

```bash
dbt run --select gold/fact
```

---

# Project Structure

```text
Walmart_Airflow_DBT_Project/
│
├── README.md
├── Notes.png
│
├── airflow_dbt_project/
│   ├── .env
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   │
│   ├── config/
│   │   └── airflow.cfg
│   │
│   ├── dags/
│   │   └── orchestrate.py
│   │
│   └── walmart_project/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       │
│       ├── macros/
│       │   └── custom_schema.sql
│       │
│       ├── models/
│       │   ├── source/
│       │   │   └── sources.yml
│       │   │
│       │   ├── silver_t/
│       │   │   ├── customers_t.sql
│       │   │   ├── employees_t.sql
│       │   │   ├── order_items_t.sql
│       │   │   ├── orders_t.sql
│       │   │   ├── products_t.sql
│       │   │   ├── stores_t.sql
│       │   │   └── properties.yml
│       │   │
│       │   ├── silver_b/
│       │   │   └── obt_b.sql
│       │   │
│       │   └── gold/
│       │       ├── ephemeral/
│       │       └── fact/
│       │
│       ├── snapshots/
│       │   ├── dim_customers.yml
│       │   ├── dim_employees.yml
│       │   ├── dim_orders.yml
│       │   ├── dim_products.yml
│       │   └── dim_stores.yml
│       │
│       └── tests/
│           └── test_obt.sql
│
└── walmart_dataset/
    ├── data/
    │   ├── customers.csv
    │   ├── employees.csv
    │   ├── order_items.csv
    │   ├── orders.csv
    │   ├── products.csv
    │   └── stores.csv
    │
    ├── ddl/
    │   └── walmart_schema.sql
    │
    └── load_data.py
```

---

# Production-Oriented Concepts

This project demonstrates several concepts commonly used in modern data engineering:

- **Airflow orchestration**
- **Databricks job orchestration**
- **Apache Spark**
- **Delta Lake**
- **Bronze/Silver/Gold architecture**
- **CDC/incremental ingestion**
- **Incremental dbt models**
- **Timestamp-based incremental loading**
- **One Big Table / business layer**
- **Jinja templating**
- **SCD Type 2 snapshots**
- **Fact modeling**
- **Data-quality tests**
- **Source freshness checks**
- **Dockerized Airflow**
- **PostgreSQL**
- **SQL and Python**
- **Data lineage through dbt model references**

---

# Important Repository Notes

### Databricks ingestion code is external

`ingest_cdc` does not implement CDC itself. It triggers an existing Databricks Job using:

```python
ws.jobs.run_now(job_id="your_databricks_job_id")
```

The Databricks job is therefore an external dependency of this repository.

### Credentials are placeholders

The repository contains placeholder values such as:

```text
your_databricks_host
your_databricks_token
your_databricks_job_id
your_databricks_http_path
```

Replace them locally and keep secrets out of source control.

### Local Docker setup

The Docker Compose configuration is intended for local development/testing, not as a production deployment.

### Airflow provider deprecation warning

The included execution logs show a deprecation warning for:

```python
from airflow.operators.bash import BashOperator
```

The current Airflow provider path should be used in a future cleanup:

```python
from airflow.providers.standard.operators.bash import BashOperator
```

---

# Validation

The repository includes Airflow execution logs from actual pipeline runs.

The logs show successful execution of key dbt stages, including:

- Databricks ingestion job completion
- Source freshness execution
- Silver Technical model build
- Silver Technical tests
- Silver Business model build
- Silver Business tests
- Gold snapshot execution

One recorded Silver Technical test run completed with:

```text
PASS=4
WARN=0
ERROR=0
```

and the Silver Business test completed with:

```text
PASS=1
WARN=0
ERROR=0
```

The logs also contain earlier failed attempts, which is useful for debugging the development history of the pipeline.

---

# Future Improvements

Potential improvements for a production deployment include:

- Move Databricks credentials to Airflow Connections/secrets
- Replace deprecated Airflow imports
- Add explicit source freshness thresholds
- Add stronger CDC/delete handling
- Add schema evolution handling
- Add more dbt data-quality tests
- Add referential-integrity tests
- Add dbt documentation and generated lineage
- Add CI/CD with GitHub Actions
- Add automated Airflow failure notifications
- Add data-quality monitoring
- Add retry/backoff configuration for external Databricks jobs
- Add environment-specific dbt profiles
- Add partitioning and Delta optimization
- Add Unity Catalog governance and permissions
- Add production observability and alerting

---

# Tech Stack

| Category | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Distributed Processing | Apache Spark |
| Lakehouse | Databricks |
| Storage Format | Delta Lake |
| Transformation | dbt |
| Source Database | PostgreSQL |
| Programming | Python |
| Query Language | SQL |
| Containerization | Docker |
| Message Broker | Redis |
| Airflow Executor | CeleryExecutor |
| Version Control | Git |
| Development | VS Code |

---

## Author

**Ayush Sinha**

Data Engineering project focused on building an orchestrated, incremental **Airflow + Databricks + dbt** pipeline with Bronze/Silver/Gold data modeling, data-quality validation, and historical snapshots.
