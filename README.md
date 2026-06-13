# Metadata-Driven BigQuery → GCS → Databricks Bronze Pipeline

## Overview

This project implements an end-to-end metadata-driven Data Engineering pipeline using:

* Google BigQuery
* Google Cloud Storage (GCS)
* Databricks
* Delta Lake
* Unity Catalog
* MySQL metadata/configuration tables

The pipeline performs:

```text
BigQuery
   ↓
Export to GCS (Parquet)
   ↓
Detect latest partition
   ↓
Stage files in Databricks Volume
   ↓
Spark transformation
   ↓
Delta Bronze Layer
   ↓
Managed Unity Catalog Tables
```

The architecture follows modern Lakehouse design principles using:

* object storage
* Delta Lake
* metadata-driven orchestration
* managed catalog tables

---

# Architecture

```text
                  ┌────────────────────┐
                  │   MySQL Metadata   │
                  │   Config Table     │
                  └─────────┬──────────┘
                            │
                            ▼
                 Fetch Eligible Tables
                            │
                            ▼
                ┌────────────────────┐
                │    Google BigQuery │
                └─────────┬──────────┘
                          │
                Export Table as Parquet
                          │
                          ▼
                ┌────────────────────┐
                │   Google Cloud     │
                │   Storage (GCS)    │
                └─────────┬──────────┘
                          │
               Detect Latest dt Partition
                          │
                          ▼
                ┌────────────────────┐
                │ Databricks Staging │
                │ Unity Catalog      │
                │ Volume             │
                └─────────┬──────────┘
                          │
                    Spark Read
                          │
                    Transformations
                          │
                          ▼
                ┌────────────────────┐
                │ Delta Bronze Layer │
                └─────────┬──────────┘
                          │
                 saveAsTable()
                          │
                          ▼
                ┌────────────────────┐
                │ Unity Catalog      │
                │ Managed Tables     │
                └────────────────────┘
```

---

# Technologies Used

| Technology           | Purpose                           |
| -------------------- | --------------------------------- |
| BigQuery             | Source system                     |
| Google Cloud Storage | Object storage / staging          |
| Databricks           | Compute engine                    |
| Spark                | Distributed data processing       |
| Delta Lake           | Lakehouse table format            |
| Unity Catalog        | Metadata and governance           |
| MySQL                | Metadata/configuration management |
| Python               | Orchestration logic               |

---

# Key Features

## 1. Metadata-Driven Pipeline

Tables are dynamically processed from a configuration table.

No hardcoded pipeline logic per table is required.

Example:

```sql
SELECT *
FROM config_table
WHERE active_flag = 1
  AND load_flag = 1
```

---

## 2. BigQuery to GCS Export

BigQuery tables are exported as Parquet files into GCS.

Example output:

```text
gs://bucket/exports/ods/ods_orders/dt=20260612180253/
```

---

## 3. Dynamic Latest Partition Detection

The pipeline automatically:

* scans GCS partitions
* detects latest `dt=`
* processes latest export only

Regex used:

```python
re.search(r"dt=(\d+)/", blob.name)
```

---

## 4. GCS Staging into Databricks

Pipeline downloads parquet files into staging directory.

Example:

```text
/Volumes/workspace/default/gcptodbmigration/gcs_stage/ods_orders
```

---

## 5. Spark-Based Transformations

Example transformation:

```python
df = df.withColumn(
    'order_ts',
    to_timestamp(col('order_ts'))
)
```

---

## 6. Delta Lake Bronze Layer

Data is stored using Delta Lake format.

Benefits:

* ACID transactions
* schema evolution
* reliable overwrites
* Delta transaction log

---

## 7. Managed Unity Catalog Tables

Tables are registered using:

```python
.saveAsTable(f"{hive_db}.{t}")
```

This automatically:

* stores Delta files
* registers metadata
* exposes SQL-accessible tables

---

# Configuration Table Design

The pipeline is fully metadata-driven.

Example schema:

| Column               | Purpose                    |
| -------------------- | -------------------------- |
| table_name           | source table               |
| source_project       | BigQuery project           |
| source_dataset       | BigQuery dataset           |
| gcs_path             | GCS export base path       |
| target_path          | Bronze layer physical path |
| active_flag          | pipeline active/inactive   |
| load_flag            | controls execution         |
| bq_to_gcs_status     | export status              |
| gcs_to_bronze_status | bronze load status         |
| error_message        | failure logging            |

---

# Pipeline Stages

# Stage 1 — BigQuery Export

Pipeline exports BigQuery tables into GCS as parquet.

Example:

```python
client.extract_table(
    full_table_name,
    destination_uri,
    job_config=job_config
)
```

Output:

```text
gs://bucket/exports/ods/ods_orders/dt=20260612180253/
```

---

# Stage 2 — Detect Latest Partition

The pipeline scans GCS objects:

```python
client.list_blobs(bucket_name, prefix=search_prefix)
```

Extracts latest partition:

```text
dt=20260612180253
```

---

# Stage 3 — GCS File Staging

Parquet files are downloaded into Databricks staging location.

Example:

```python
blob.download_to_filename(local_file)
```

---

# Stage 4 — Spark Read

Spark loads parquet files:

```python
spark.read.parquet(stage_dir)
```

---

# Stage 5 — Data Transformation

Timestamp normalization:

```python
to_timestamp(col('order_ts'))
```

---

# Stage 6 — Delta Bronze Write

Delta write operation:

```python
df.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .format("delta")
```

---

# Stage 7 — Unity Catalog Registration

Managed table creation:

```python
.saveAsTable(f"{hive_db}.{t}")
```

---

# Lakehouse Concepts Used

This project implements core Lakehouse principles:

| Concept           | Implementation      |
| ----------------- | ------------------- |
| Object Storage    | GCS                 |
| Open Format       | Parquet             |
| Transaction Layer | Delta Lake          |
| Compute Layer     | Spark               |
| Catalog Layer     | Unity Catalog       |
| Bronze Layer      | Raw Delta ingestion |

---

# Delta Lake Internals

Delta tables physically contain:

```text
_delta_log/
part-00000.parquet
```

Where:

* parquet files contain actual data
* `_delta_log` tracks transactions and schema

---

# Error Handling

Pipeline tracks failures inside metadata table.

Statuses:

* IN_PROGRESS
* COMPLETED
* FAILED

Errors stored in:

```sql
error_message
```

---

# Design Patterns Used

## Metadata-Driven ETL

Pipeline behavior controlled through metadata table.

---

## Dynamic Partition Discovery

Latest partition automatically detected.

---

## Bronze Layer Architecture

Raw ingestion layer stored in Delta format.

---

## Context Manager Pattern

Used for MySQL connection handling:

```python
@contextmanager
```

---

# Challenges Solved

## 1. Regex Partition Detection

Issue:

* partition naming mismatch

Solution:

* generalized regex parsing

---

## 2. Unity Catalog Path Handling

Issue:

* LOCATION syntax issues

Solution:

* managed Unity Catalog tables via `saveAsTable()`

---

## 3. GCS Object Discovery

Issue:

* wildcard path handling

Solution:

* prefix-based blob listing

---


# Sample Bronze Query

```sql
SELECT *
FROM bronze.ods_orders
LIMIT 10
```

---

# Learning Outcomes

This project demonstrates:

* End-to-end ETL design
* Lakehouse architecture
* Delta Lake fundamentals
* Spark transformations
* Metadata-driven orchestration
* Cloud object storage handling
* Unity Catalog usage
* Partition-based ingestion
* Real-world debugging techniques

---

## Acknowledgement

The foundational pipeline structure and learning guidance for this project were inspired by live sessions conducted by **Anurag Srivastava** during my **DataX bootcamp**.
I further extended, debugged, adapted, and implemented the pipeline independently for modern Databricks free edition Unity Catalog and Delta Lake workflows.


# Conclusion

This project implements a modern cloud-native Data Engineering pipeline using:

* BigQuery
* GCS
* Databricks
* Spark
* Delta Lake
* Unity Catalog

The architecture follows real-world Lakehouse design patterns and demonstrates:

* metadata-driven orchestration
* scalable ingestion
* partition-aware processing
* Delta-based bronze layer creation
* governed catalog-based table management
