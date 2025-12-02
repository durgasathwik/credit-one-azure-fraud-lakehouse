# Credit One – Azure Fraud Lakehouse (Practice Project)

This repo is a hands-on practice implementation of a modern data & MLOps stack inspired by my Credit One Bank experience.

Goals:
- Simulate credit-card/fraud transactions.
- Build a medallion-style lakehouse (bronze/silver/gold) using Spark (local first).
- Add streaming (local Kafka via Docker).
- Later: connect to free-tier Azure/Databricks/MLflow when needed.

Folder structure:
- `src/`       – Python modules, utilities, data generators
- `notebooks/` – Exploration & Spark notebooks
- `data/`      – Local sample datasets (do not commit large real data)
- `docker/`    – Docker Compose files for Kafka, Spark, etc.
- `infra/`     – IaC or scripts (optional)
- `docs/`      – Diagrams, design notes
