# Discogs Lakehouse + Graph data platform

A distributed **Lakehouse + Graph data platform** built on top of
Discogs monthly snapshot dumps (multi-gigabyte XML datasets).

This project processes large-scale music metadata using Spark and Delta
Lake, models artist collaborations as a graph inspired by the **six
degrees of separation** concept, and enables both relational analytics
and graph-based exploration.

The architecture focuses on incremental processing, idempotency, schema
control, and production-style engineering practices.

------------------------------------------------------------------------

# 🎯 Project Objectives

-   Process multi-GB monthly snapshot dumps efficiently
-   Ensure idempotent and incremental ingestion
-   Minimize unnecessary writes
-   Implement a Medallion architecture (Raw → Bronze → Silver → Gold)
-   Provide relational analytics (PostgreSQL)
-   Prepare graph-ready datasets (Neo4j)
-   Enable collaboration and genre network analysis
-   Maintain environment-aware, config-driven pipelines

------------------------------------------------------------------------

# 🏗 Architecture Overview
```
Discogs Monthly Snapshot (XML)
              ↓
Raw Layer (MinIO - S3 compatible)
              ↓
Bronze Layer (Structured Delta Tables)
              ↓
Silver Layer (Normalized Domain Model)
              ↓
Gold Layer (Analytics + Graph-Ready Tables)
              ↓
PostgreSQL (analytics) + Neo4j (graph)
```
------------------------------------------------------------------------

# 🔁 Snapshot-Based Incremental Strategy

Discogs provides **full monthly snapshots**, not CDC logs.

The ingestion strategy is designed accordingly:

-   Checksum validation before processing
-   Date-aware logic to avoid outdated dump ingestion
-   Hash-based change detection
-   Delta Lake MERGE operations
-   Write minimization strategy
-   Idempotent re-runnable pipeline

The latest dump is treated as the single source of truth.
Backfills are intentionally not required in the current design.

------------------------------------------------------------------------

# 📦 Medallion Architecture

## Raw Layer

-   Snapshot download
-   Checksum verification
-   Structured object storage in MinIO
-   Snapshot version tracking

## Bronze Layer (In Progress)

-   Distributed XML parsing with Spark
-   Explicit schema application
-   Nested structure serialization to JSON (to guarantee stable Delta
    merge behavior)
-   Delta table creation
-   Incremental merge logic
-   Operation statistics collection (inserted / updated / deleted rows)
-   Structured JSON logging
-   Processing duration tracking

Schema drift at this layer is handled via nested structure serialization
to preserve merge stability.

## Silver Layer

-   Enforced domain schema
-   Normalized entities (Artists, Releases)
-   Collaboration extraction
-   Role filtering capability
-   Removal of non-relevant fields
-   Label data intentionally excluded

Schema enforcement occurs here to ensure clean domain modeling.

## Gold Layer (In Progress)

-   Aggregated measures
-   Genre-level metrics
-   Time-series analytics
-   Graph-ready edge datasets

------------------------------------------------------------------------

# 🛠 Technology Stack

| Layer                | Technology                       |
| -------------------- | -------------------------------- |
| Language             | Python                           |
| Distributed Compute  | Apache Spark                     |
| Storage              | Delta Lake                       |
| Object Storage       | MinIO (S3-compatible)            |
| Metadata Governance  | Unity Catalog (self-hosted)      |
| Relational Analytics | PostgreSQL (planned integration) |
| Graph Modeling       | Neo4j (planned integration)      |
| Orchestration        | Apache Airflow (planned)         |
| Containerization     | Docker & Docker Compose          |

Unity Catalog objects are initialized via `uc_init.py`.

------------------------------------------------------------------------

# 📊 Relational + Graph Modeling Strategy

## Relational Modeling

Used for:

-   Aggregated metrics (albums per artist, releases per decade)
-   Genre distribution analysis
-   Artist productivity measurement
-   Genre popularity evolution across decades
-   Planned PostgreSQL full-text search for artist discovery and
    filtering

## Graph Modeling

Node Types:
- Artist
- Release

Edge Type:
- PARTICIPATED_IN (role as attribute)

Role attributes allow:

-   Filtering out non-musical roles (e.g. designer)
-   Building performance-only collaboration graphs
-   Temporal slicing of networks
-   Genre-to-genre influence mapping
-   Shortest path queries between artists

The design is inspired by the six degrees of separation concept applied
to the global music ecosystem.

------------------------------------------------------------------------

# 📈 Observability & Engineering Practices

## Implemented

-   Structured JSON logging
-   Delta operation statistics (inserted / updated / deleted rows)
-   Processing duration metrics
-   Integration test on small sample dataset
-   Manual linting via Ruff
-   Environment separation via JSON configs
-   Config-driven pipeline behavior
-   Modular ingestion helpers
-   Dockerized infrastructure

## Planned

-   CI/CD pipeline (GitLab runner on sample dataset)
-   Prometheus/Grafana monitoring (Airflow DAG metrics)
-   Expanded schema drift monitoring
-   Data quality validation layer

------------------------------------------------------------------------

# ⚙️ Configuration & Environments

-   Separate configuration files per layer
-   Environment-based behavior (dev / test)
-   Parameterized Spark session builder
-   Centralized ingestion logic
-   Reproducible containerized execution

------------------------------------------------------------------------

# 🚀 Roadmap

-   Airflow DAG orchestration
-   PostgreSQL integration for analytics
-   Neo4j graph loading
-   Role-filtered collaboration network
-   Genre-level aggregation tables
-   Graph performance optimization
-   Web interface for exploration
-   Collaboration path exploration game

------------------------------------------------------------------------

# 🧠 Engineering Themes Demonstrated

-   Snapshot-based incremental ingestion without CDC logs
-   Distributed XML parsing at scale
-   Lakehouse architecture implementation
-   Idempotent pipeline design
-   Schema control across layers
-   Structured logging & metrics
-   Hybrid relational + graph data modeling
-   Containerized reproducible infrastructure
-   Production-style configuration management
