# Real-Time Transaction Risk Platform

A real-world fintech data engineering project that simulates a transaction risk scoring platform using Python, PySpark, Parquet, deduplication, risk scoring, and Gold-level reporting outputs.

## Business Problem

Fintech and payment platforms process high-volume transaction events from mobile apps, web apps, card networks, and payment gateways. These records can contain duplicate transaction IDs, invalid amounts, delayed events, and high-risk transaction patterns.

This project demonstrates how a data engineer can build a reliable transaction pipeline that cleans raw events, scores risk, and creates analytics-ready outputs for fraud and risk teams.

## Project Objective

Build a PySpark transaction risk platform that:

- Generates raw transaction events
- Stores raw transaction data in a Bronze layer
- Removes duplicate transactions in a Silver layer
- Applies rule-based risk scoring
- Assigns transaction risk bands
- Creates Gold-level risk summaries for reporting

## Architecture

```text
Raw Transaction Events
        ↓
Bronze Layer
Raw transactions stored as Parquet
        ↓
Silver Layer
Deduplicated and risk-scored transactions
        ↓
Gold Layer
Risk summary by risk band and payment method
```

## Tech Stack

- Python 3.11
- PySpark
- Spark SQL DataFrame API
- Pandas
- Parquet
- Risk scoring rules
- Deduplication
- Bronze/Silver/Gold architecture
- Git/GitHub

## Dataset

The generated transaction dataset includes:

- transaction_id
- customer_id
- event_time
- amount
- merchant
- location
- payment_method

The generator intentionally creates duplicate transaction records so the pipeline can prove deduplication behavior.

## Pipeline Layers

### Bronze Layer

Stores raw transaction events with a load timestamp.

### Silver Layer

Applies validation and enrichment:

- transaction_id must not be null
- customer_id must not be null
- amount must be greater than zero
- duplicate transaction_id records are removed
- risk_score is calculated from transaction amount
- risk_band is assigned as LOW, MEDIUM, or HIGH

### Gold Layer

Creates reporting metrics:

- transaction count
- total transaction amount
- average transaction amount
- grouped by risk_band and payment_method

## How to Run

```bash
python3.11 -m venv venv
source venv/bin/activate
pip install pyspark pandas

python src/run_pipeline.py
python src/pyspark_transaction_risk_pipeline.py
python src/project_summary.py
```

## Expected Outputs

```text
data/output/bronze_transactions_parquet
data/output/silver_transactions_scored_parquet
data/output/gold_transaction_risk_summary_parquet
```

## Project Summary Script

`src/project_summary.py` validates the pipeline output by showing:

- raw record count
- Bronze count
- Silver count
- Gold count
- duplicate transaction_id proof before and after deduplication
- invalid record proof before and after validation
- risk band distribution
- sample Gold output

## Key Data Engineering Concepts Demonstrated

- PySpark batch processing
- Explicit schema enforcement
- Transaction event processing
- Deduplication using transaction_id
- Rule-based feature engineering
- Risk scoring and risk-band assignment
- Gold reporting layer creation
- Parquet analytical outputs

## Resume Bullet

Built a PySpark transaction risk scoring platform that processes raw payment events, removes duplicate transaction IDs, assigns rule-based risk scores, and publishes Bronze, Silver, and Gold Parquet layers for fraud and risk analytics.
