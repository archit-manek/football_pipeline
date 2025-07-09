## Football Analytics Pipeline

A modular data pipeline for ingesting, transforming, and engineering features from football (soccer) match event data.

Built using Medallion architecture and modern Python tooling, this project is designed as a foundation for building analytics and machine learning models, such as xG estimation and player performance analysis on structured event data.

---

### Architecture

The pipeline uses a three layer Medallion structure:

* **Bronze** – Raw StatsBomb JSON files
* **Silver** – Cleaned nested columns and derived new columns
* **Gold** – Feature-engineered tables for modeling (e.g., shot angle, distance, goal label)

---

### Technologies Used

* `polars` for transformation
* `parquet` for storage
* StatsBomb Open Data for event-level input

---

### Data Directory Layout

```
data/
├── bronze/    # Raw match/event JSONs (from StatsBomb)
├── silver/    # Cleaned data tables
└── gold/      # Engineered features
```
