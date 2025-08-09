## Football Analytics Pipeline

A modular data pipeline for ingesting, transforming, and engineering features from football (soccer) match event data.

Built using Medallion architecture and modern Python tooling, this project is designed as a foundation for building analytics and machine learning models, such as xG estimation and player performance analysis on structured event data.

---

### Architecture

The pipeline uses a three layer Medallion structure:

* **Bronze** – Raw StatsBomb JSON files
* **Silver** – Cleaned nested columns and derived new columns
* **Gold** – Feature-engineered tables for modeling (e.g., shot angle, distance, goal label)

## Setup (for contributors)

```bash
pip install -e .
python src/football_pipeline/tools/update_requirements.py  # Only needed if you want requirements.txt