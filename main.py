from football_pipeline.pipeline import run_pipeline

if __name__ == "__main__":
    # Defaults: bronze only, open_data
    run_pipeline(bronze=True, silver=False, gold=False)