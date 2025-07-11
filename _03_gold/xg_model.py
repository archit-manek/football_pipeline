import logging
import numpy as np
import polars as pl
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, log_loss, classification_report, roc_curve
from sklearn.calibration import calibration_curve
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Tuple, Dict, Any, Optional
import warnings
warnings.filterwarnings('ignore')

from utils.constants import SILVER_DIR_EVENTS, GOLD_DIR
from utils.logging import setup_logger

# Set up logging
log_path = Path("logs/gold/xg_model.log")
logger = setup_logger(log_path, "xg_model")

class XGModel:
    """
    Expected Goals (xG) model for football shot analysis.
    
    This class handles:
    - Feature engineering from shot events
    - Model training and evaluation
    - Comparison with StatsBomb xG
    - Model persistence and loading
    """
    
    def __init__(self):
        self.model = None
        self.feature_columns = []
        self.gold_dir = Path(GOLD_DIR)
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        
    def load_shot_data(self, max_files: Optional[int] = None) -> pl.DataFrame:
        """
        Load shot data from all silver events files.
        
        Args:
            max_files: Maximum number of files to process (None for all)
            
        Returns:
            DataFrame with all shot events
        """
        logger.info("Loading shot data from silver events...")
        
        events_dir = Path(SILVER_DIR_EVENTS)
        shot_data = []
        
        files_processed = 0
        for parquet_file in events_dir.glob("*.parquet"):
            if max_files and files_processed >= max_files:
                break
                
            try:
                df = pl.read_parquet(parquet_file)
                shots = df.filter((pl.col("type_name") == "Shot") & (pl.col("shot_type_name") != "Penalty"))
                
                if len(shots) > 0:
                    shot_data.append(shots)
                    files_processed += 1
                    
                    if files_processed % 100 == 0:
                        logger.info(f"Processed {files_processed} files...")
                        
            except Exception as e:
                logger.error(f"Error processing {parquet_file}: {e}")
                continue
        
        if not shot_data:
            raise ValueError("No shot data found in silver events!")
            
        # Combine shots with schema alignment
        combined_shots = pl.concat(shot_data, how="diagonal")
        logger.info(f"Loaded {len(combined_shots)} shots from {files_processed} files")
        
        return combined_shots
    
    def engineer_features(self, shots: pl.DataFrame) -> pl.DataFrame:
        """
        Engineer features for xG model.
        
        Args:
            shots: DataFrame with shot events
            
        Returns:
            DataFrame with engineered features
        """
        logger.info("Engineering features for xG model...")
        
        # Create target variable (1 for goal, 0 for no goal)
        shots = shots.with_columns(
            pl.when(pl.col("shot_outcome_name") == "Goal")
            .then(1)
            .otherwise(0)
            .alias("goal")
        )
        
        # Distance from goal (using shot location)
        # Note: StatsBomb coordinates are normalized (0-1), so we convert to raw coordinates
        # StatsBomb uses 120x80 system: goal at x=120, y=40 (center)
        shots = shots.with_columns(
            pl.when(pl.col("x").is_not_null() & pl.col("y").is_not_null())
            .then(((120 - pl.col("x") * 120) ** 2 + (40 - pl.col("y") * 80) ** 2) ** 0.5)
            .otherwise(None)
            .alias("distance_to_goal")
        )
        
        # Angle to goal
        # Convert normalized coordinates to raw coordinates for angle calculation
        shots = shots.with_columns(
            pl.when(pl.col("x").is_not_null() & pl.col("y").is_not_null())
            .then(
                # Calculate the visual angle between the shot and the goal
                pl.arctan2(
                    # Goal width in StatsBomb units (7.32m)
                    pl.lit(7.32),
                    # Calculate distance to BOTH goal posts (3.66m from center)
                    pl.max_horizontal([
                        ((120 - pl.col("x") * 120) ** 2 + (40 - 3.66 - pl.col("y") * 80) ** 2) ** 0.5,
                        ((120 - pl.col("x") * 120) ** 2 + (40 + 3.66 - pl.col("y") * 80) ** 2) ** 0.5
                    ])
                ) * 180 / np.pi
            )
            .otherwise(None)
            .alias("angle_to_goal")
        )
        
        # Binary features
        shots = shots.with_columns([
            pl.col("under_pressure").fill_null(False).alias("under_pressure"),
            pl.col("shot_first_time").fill_null(False).alias("shot_first_time"),
            pl.col("shot_one_on_one").fill_null(False).alias("shot_one_on_one"),
        ])
        
        # Categorical features
        body_parts = shots.select("shot_body_part_name").unique().to_pandas()["shot_body_part_name"].dropna().tolist()
        techniques = shots.select("shot_technique_name").unique().to_pandas()["shot_technique_name"].dropna().tolist()
        
        # Create dummy variables for body parts
        for body_part in body_parts:
            shots = shots.with_columns(
                pl.when(pl.col("shot_body_part_name") == body_part)
                .then(1)
                .otherwise(0)
                .alias(f"body_part_{body_part.lower().replace(' ', '_')}")
            )
        
        # Create dummy variables for techniques
        for technique in techniques:
            shots = shots.with_columns(
                pl.when(pl.col("shot_technique_name") == technique)
                .then(1)
                .otherwise(0)
                .alias(f"technique_{technique.lower().replace(' ', '_')}")
            )
        
        logger.info(f"Engineered features for {len(shots)} shots")
        return shots
    
    def prepare_training_data(self, shots: pl.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Prepare training data for the model.
        
        Args:
            shots: DataFrame with engineered features
            
        Returns:
            Tuple of (X, y) arrays
        """
        logger.info("Preparing training data...")
        
        # Define feature columns
        feature_columns = [
            "distance_to_goal",
            "angle_to_goal",
            "under_pressure",
            "shot_first_time",
            "shot_one_on_one",
        ]
        
        # Add body part and technique dummy variables
        dummy_cols = [col for col in shots.columns if col.startswith(("body_part_", "technique_"))]
        feature_columns.extend(dummy_cols)
        
        # Filter to rows with non-null essential features
        training_data = shots.filter(
            pl.col("distance_to_goal").is_not_null() &
            pl.col("angle_to_goal").is_not_null() &
            pl.col("goal").is_not_null()
        )
        
        # Convert to pandas for sklearn compatibility
        training_df = training_data.select(feature_columns + ["goal"]).to_pandas()
        
        # Handle any remaining nulls
        training_df = training_df.fillna(0)
        
        X = np.array(training_df[feature_columns].values)
        y = np.array(training_df["goal"].values)
        
        self.feature_columns = feature_columns
        
        logger.info(f"Prepared training data: {X.shape[0]} samples, {X.shape[1]} features")
        logger.info(f"Goal rate: {y.mean():.1%}")
        
        return X, y
    
    def train_model(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """
        Train the xG model and return evaluation metrics.
        
        Args:
            X: Feature matrix
            y: Target variable
            
        Returns:
            Dictionary with evaluation metrics
        """
        logger.info("Training xG model...")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Train multiple models
        models = {
            "logistic_regression": LogisticRegression(random_state=42, max_iter=1000),
            "random_forest": RandomForestClassifier(
                n_estimators=50, 
                max_depth=8, 
                min_samples_split=20, 
                random_state=42, 
                n_jobs=-1
            )
        }
        
        results = {}
        
        for name, model in models.items():
            logger.info(f"Training {name}...")
            
            # Train model
            model.fit(X_train, y_train)
            
            # Predictions
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            
            # Evaluate
            auc = roc_auc_score(y_test, y_pred_proba)
            logloss = log_loss(y_test, y_pred_proba)
            
            results[name] = {
                "model": model,
                "auc": auc,
                "log_loss": logloss,
                "y_test": y_test,
                "y_pred_proba": y_pred_proba
            }
            
            logger.info(f"{name} - AUC: {auc:.3f}, Log Loss: {logloss:.3f}")
        
        # Select best model (by AUC)
        best_model_name = max(results.keys(), key=lambda x: results[x]["auc"])
        self.model = results[best_model_name]["model"]
        
        logger.info(f"Best model: {best_model_name}")
        
        return results
    
    def evaluate_against_statsbomb(self, shots: pl.DataFrame) -> Dict[str, float]:
        """
        Evaluate our model against StatsBomb xG.
        
        Args:
            shots: DataFrame with StatsBomb xG values
            
        Returns:
            Dictionary with comparison metrics
        """
        logger.info("Evaluating against StatsBomb xG...")
        
        # Get predictions on full dataset
        evaluation_data = shots.filter(
            pl.col("distance_to_goal").is_not_null() &
            pl.col("angle_to_goal").is_not_null() &
            pl.col("goal").is_not_null() &
            pl.col("shot_statsbomb_xg").is_not_null()
        )
        
        eval_df = evaluation_data.select(self.feature_columns + ["goal", "shot_statsbomb_xg"]).to_pandas()
        eval_df = eval_df.fillna(0)
        
        X_eval = eval_df[self.feature_columns].values
        y_eval = eval_df["goal"].values
        statsbomb_xg = eval_df["shot_statsbomb_xg"].values
        
        # Our model predictions
        if self.model is None:
            raise ValueError("Model not trained yet!")
        our_xg = self.model.predict_proba(X_eval)[:, 1]
        
        # Compare metrics
        our_auc = roc_auc_score(y_eval, our_xg)
        statsbomb_auc = roc_auc_score(y_eval, statsbomb_xg)
        
        our_logloss = log_loss(y_eval, our_xg)
        statsbomb_logloss = log_loss(y_eval, statsbomb_xg)
        
        comparison = {
            "our_auc": our_auc,
            "statsbomb_auc": statsbomb_auc,
            "our_log_loss": our_logloss,
            "statsbomb_log_loss": statsbomb_logloss,
            "correlation": np.corrcoef(np.array(our_xg), np.array(statsbomb_xg))[0, 1]
        }
        
        logger.info(f"Our model AUC: {our_auc:.3f}, StatsBomb AUC: {statsbomb_auc:.3f}")
        logger.info(f"Correlation with StatsBomb: {comparison['correlation']:.3f}")
        
        return comparison
    
    def save_model(self, model_name: str = "xg_model_v1"):
        """Save the trained model."""
        if self.model is None:
            raise ValueError("No model to save. Train the model first.")
        
        model_path = self.gold_dir / f"{model_name}.pkl"
        joblib.dump({
            "model": self.model,
            "feature_columns": self.feature_columns
        }, model_path)
        
        logger.info(f"Model saved to {model_path}")
    
    def load_model(self, model_name: str = "xg_model_v1"):
        """Load a saved model."""
        model_path = self.gold_dir / f"{model_name}.pkl"
        
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
        
        model_data = joblib.load(model_path)
        self.model = model_data["model"]
        self.feature_columns = model_data["feature_columns"]
        
        logger.info(f"Model loaded from {model_path}")
    
    def predict_xg(self, shot_features: dict) -> float:
        """
        Predict xG for a single shot.
        
        Args:
            shot_features: Dictionary with shot features
            
        Returns:
            Predicted xG value
        """
        if self.model is None:
            raise ValueError("No model loaded. Train or load a model first.")
        
        # Convert features to array
        feature_array = np.zeros(len(self.feature_columns))
        for i, col in enumerate(self.feature_columns):
            feature_array[i] = shot_features.get(col, 0)
        
        return self.model.predict_proba(feature_array.reshape(1, -1))[0, 1]


def build_xg_model(max_files: Optional[int] = None) -> XGModel:
    """
    Main function to build and train the xG model.
    
    Args:
        max_files: Maximum number of files to process (None for all)
        
    Returns:
        Trained XGModel instance
    """
    logger.info("Starting xG model build process...")
    
    # Initialize model
    xg_model = XGModel()
    
    # Load shot data
    shots = xg_model.load_shot_data(max_files=max_files)
    
    # Engineer features
    shots = xg_model.engineer_features(shots)
    
    # Prepare training data
    X, y = xg_model.prepare_training_data(shots)
    
    # Train model
    results = xg_model.train_model(X, y)
    
    # Evaluate against StatsBomb
    comparison = xg_model.evaluate_against_statsbomb(shots)
    
    # Save model
    xg_model.save_model()
    
    logger.info("xG model build completed successfully!")
    
    return xg_model


if __name__ == "__main__":
    # Build the model with a subset of data for testing
    model = build_xg_model(max_files=100)  # Use max_files=None for full dataset 