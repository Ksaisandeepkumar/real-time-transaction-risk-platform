# Real-Time Transaction Risk Platform

## Business Problem
Financial platforms process transaction events that may be duplicated, delayed, high-value, or risky. Risk teams need clean and scored transaction data for fraud monitoring and operational reporting.

## Objective
Build a transaction risk pipeline that generates events, removes duplicates, assigns risk scores, and produces risk-band summaries.

## Architecture
```text
Raw Transactions
  -> Deduplicated Transactions
  -> Risk Scoring Layer
  -> Gold Risk Summary
```

## Key Data Engineering Concepts
- Transaction event processing
- Duplicate removal
- Risk scoring logic
- Feature generation
- Gold analytics output

## How to Run
```bash
python src/run_pipeline.py
```

## Resume Bullet
Built a transaction risk scoring platform that processes payment events, removes duplicate transactions, assigns rule-based risk scores, and produces Gold-level risk-band summaries for fraud analytics.
