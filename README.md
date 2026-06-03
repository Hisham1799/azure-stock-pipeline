# Azure Stock Data Pipeline

An automated, production-grade financial batch data pipeline built on Azure, demonstrating the full modern data engineering stack.

## Architecture

![Pipeline Architecture](architecture.png)

## What This Pipeline Does

1. **Ingest:** Azure Data Factory pulls daily OHLCV stock data (AAPL, MSFT, GOOGL, TSLA, NVDA, AMZN) from the Twelve Data REST API and lands raw JSON in the bronze layer
2. **Transform:** A Databricks PySpark notebook cleans, flattens, and writes the silver Delta table, then aggregates business KPIs into the gold Delta table
3. **Serve (Phase 3):** Azure Synapse Analytics serverless SQL pool will expose gold Delta tables as external tables for analyst queries

The pipeline runs automatically every day at midnight IST via a schedule trigger.

## Tech Stack

| Component | Service |
|---|---|
| Orchestration | Azure Data Factory V2 |
| Storage | Azure Data Lake Storage Gen2 (hierarchical namespace) |
| Transformation | Azure Databricks (PySpark, Delta Lake) |
| Serving | Azure Synapse Analytics serverless SQL *(Phase 3)* |
| Data Source | Twelve Data REST API |

## Medallion Architecture

## Gold Layer Output (sample)

| symbol | average_closing_price | highest_all_time_peak | total_volume_traded |
|---|---|---|---|
| AMZN | 234.82 | 278.56 | 4,791,108,574 |
| TSLA | 404.43 | 454.29 | 5,970,586,830 |
| NVDA | 192.93 | 236.53 | 17,068,374,682 |

## ADF Pipeline Structure

- **`pl_ingest_stocks_bronze`** — parameterized pipeline (symbols parameter)
  - `Copy data1` — REST source → ADLS bronze sink
  - `nb_bronze_to_gold` — Databricks notebook activity (success dependency)
- **`tr_daily_stock_ingest`** — schedule trigger, daily midnight IST

## Key Engineering Decisions

**Intentional tech debt:** Storage account keys and API credentials are currently hardcoded. Phase 3 refactors these into Azure Key Vault + Managed Identity — a deliberate before/after talking point demonstrating security maturity progression.

**Cost discipline:** Single-node Databricks cluster with 10-minute auto-termination. All pipeline runs use jobs compute. Dataset size kept at 5–50 MB micro-batches.

**Delta Lake over plain Parquet:** Silver and gold layers use Delta format for ACID guarantees, time travel, and schema enforcement — enabling the incremental load pattern in Phase 3.
## Notebooks

| Notebook | Purpose |
|---|---|
| `databricks/01_bronze_to_silver.py` | Ingests bronze JSON, writes silver Delta, aggregates gold Delta KPIs |
| `databricks/02_incremental_scd2.py` | SCD Type 2 incremental load using Delta MERGE — preserves full price history |

## SQL Scripts

| Script | Purpose |
|---|---|
| `synapse/sql_serve_gold_stock_summary.sql` | Creates external table over gold Delta in Synapse serverless SQL pool |
## Planned: Phase 3

- [ ] Synapse serverless SQL external tables over gold Delta
- [ ] Azure Key Vault + Managed Identity (remove hardcoded credentials)
- [ ] Incremental load + SCD Type 2 via Delta MERGE
- [ ] Git integration with ADF

## Resource Group

All services provisioned in `rg-de-sprint`, Central India region.
