# Soccer Analytics Pipeline

A modular data pipeline for ingesting, transforming, and engineering features from football (soccer) match event data using Medallion architecture and modern Python tooling.

This project is designed to serve as the foundation for building analytics and machine learning models (e.g., xG, player performance analysis) on top of structured event data.

---

## Architecture

The pipeline follows the **Medallion architecture** with three layers of transformation:

Current Approach:
- **Bronze**: Raw JSON files from StatsBomb Open Data
- **Silver**: Cleaned, flattened, and normalized event data (e.g., shots, passes)
- **Gold**: Engineered features ready for downstream analytics and model training (e.g., shot angle, distance, goal label)

---

## Technologies Used So Far

- **Pandas** – DataFrame engine for parsing and transformation
- **Parquet** – columnar data format for Silver and Gold layers
- **StatsBomb Open Data** – publicly available football match data

---

## Data Flow

```text
data/
├── bronze/    <- Raw match/event JSONs (from StatsBomb)
├── silver/    <- Normalized event-level data (e.g. matches.parquet)
└── gold/      <- Features for modeling (e.g. xG-ready tables)
