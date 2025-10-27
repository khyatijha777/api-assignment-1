"""
Prefect ML Pipeline for Heart Disease Prediction
Converts the original ML pipeline to use Prefect flows and tasks
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    classification_report, confusion_matrix, roc_auc_score
)
from prefect import flow, task, get_run_logger
from typing import Dict, Any, Tuple, List
import joblib
import os
from datetime import datetime
import json


@task(name="load_processed_data", retries=2, retry_delay_seconds=5)
def load_processed_data(file_path: str = "heart_processed.csv") -> pd.DataFrame:
    """
    Load the processed heart disease dataset
    """
    logger = get_run_logger()
    logger.info(f"Loading processed data from {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded processed data with shape: {df.shape}")
        logger.info(f"Columns: {list(df.columns)}")
        return df
    except FileNotFoundError:
        logger.error(f"File {file_path} not found. Run data pipeline first.")
        raise
    except Exception as e:
        logger.error(f"Error loading processed data: {str(e)}")
        raise


@task(name="prepare_features_and_target", retries=1)
def prepare_features_and_target(df: pd.DataFrame, target_column: str = "target") -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare features (X) and target (y) for model training
    """
    logger = get_run_logger()
    
    if target_column not in df.columns:
        logger.error(f"Target column '{target_column}' not found in dataset")
        raise ValueError(f"Target column '{target_column}' not found")
    
    X = df.drop(target_column, axis=1)
    y = df[target_column]
    
    logger.info(f"Features shape: {X.shape}")
    logger.info(f"Target shape: {y.shape}")
    logger.info(f"Target distribution: {y.value_counts().to_dict()}")
    
    return X, y


@task(name="split_data", retries=1)
def split_data(X: pd.DataFrame, y: pd.Series, test_size: float = 0.3, random_state: int = 42) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """
    Split data into training and testing sets
    """
    logger = get_run_logger()
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )
    
    logger.info(f"Training set shape: {X_train.shape}")
    logger.info(f"Test set shape: {X_test.shape}")
    logger.info(f"Training target distribution: {y_train.value_counts().to_dict()}")
    logger.info(f"Test target distribution: {y_test.value_counts().to_dict()}")
    
    return X_train, X_test, y_train, y_test


@task(name="train_logistic_regression", retries=1)
def train_logistic_regression(X_train: pd.DataFrame, y_train: pd.Series) -> Tuple[LogisticRegression, Dict[str, Any]]:
    """
    Train Logistic Regression model
    """
    logger = get_run_logger()
    logger.info("Training Logistic Regression model")
    
    model = LogisticRegression(max_iter=500, random_state=42)
    model.fit(X_train, y_train)
    
    # Cross-validation score
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='accuracy')
    
    model_info = {
        "model_name": "Logistic Regression",
        "parameters": model.get_params(),
        "cv_mean_score": cv_scores.mean(),
        "cv_std_score": cv_scores.std(),
        "training_samples": len(X_train)
    }
    
    logger.info(f"Logistic Regression CV Score: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
    
    return model, model_info


@task(name="train_random_forest", retries=1)
def train_random_forest(X_train: pd.DataFrame, y_train: pd.Series) -> Tuple[RandomForestClassifier, Dict[str, Any]]:
    """
    Train Random Forest model
    """
    logger = get_run_logger()
    logger.info("Training Random Forest model")
    
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Cross-validation score
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='accuracy')
    
    model_info = {
        "model_name": "Random Forest",
        "parameters": model.get_params(),
        "cv_mean_score": cv_scores.mean(),
        "cv_std_score": cv_scores.std(),
        "training_samples": len(X_train),
        "feature_importances": dict(zip(X_train.columns, model.feature_importances_))
    }
    
    logger.info(f"Random Forest CV Score: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
    
    return model, model_info


@task(name="evaluate_model", retries=1)
def evaluate_model(model: Any, X_test: pd.DataFrame, y_test: pd.Series, model_name: str) -> Dict[str, Any]:
    """
    Evaluate model performance on test set
    """
    logger = get_run_logger()
    logger.info(f"Evaluating {model_name}")
    
    # Make predictions
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, 'predict_proba') else None
    
    # Calculate metrics
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, average='weighted'),
        "recall": recall_score(y_test, y_pred, average='weighted'),
        "f1_score": f1_score(y_test, y_pred, average='weighted')
    }
    
    # Add AUC if probabilities are available
    if y_pred_proba is not None:
        metrics["roc_auc"] = roc_auc_score(y_test, y_pred_proba)
    
    # Classification report
    classification_rep = classification_report(y_test, y_pred, output_dict=True)
    metrics["classification_report"] = classification_rep
    
    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    metrics["confusion_matrix"] = cm.tolist()
    
    logger.info(f"{model_name} Metrics:")
    for metric, value in metrics.items():
        if metric not in ["classification_report", "confusion_matrix"]:
            logger.info(f"  {metric}: {value:.4f}")
    
    return metrics


@task(name="save_model", retries=2)
def save_model(model: Any, model_name: str, metrics: Dict[str, Any], output_dir: str = "models") -> str:
    """
    Save trained model and its metrics
    """
    logger = get_run_logger()
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save model
    model_filename = f"{model_name.lower().replace(' ', '_')}_model.pkl"
    model_path = os.path.join(output_dir, model_filename)
    joblib.dump(model, model_path)
    
    # Save metrics
    metrics_filename = f"{model_name.lower().replace(' ', '_')}_metrics.json"
    metrics_path = os.path.join(output_dir, metrics_filename)
    
    # Prepare metrics for JSON serialization
    metrics_json = {
        "model_name": model_name,
        "timestamp": datetime.now().isoformat(),
        "metrics": {k: v for k, v in metrics.items() if k not in ["classification_report", "confusion_matrix"]},
        "confusion_matrix": metrics.get("confusion_matrix", []),
        "classification_report": metrics.get("classification_report", {})
    }
    
    with open(metrics_path, 'w') as f:
        json.dump(metrics_json, f, indent=2)
    
    logger.info(f"Model saved to: {model_path}")
    logger.info(f"Metrics saved to: {metrics_path}")
    
    return model_path


@flow(
    name="heart-disease-ml-pipeline",
    description="Complete ML training and evaluation pipeline for heart disease prediction"
)
def heart_disease_ml_pipeline(
    processed_data_file: str = "heart_processed.csv",
    test_size: float = 0.3,
    random_state: int = 42,
    models_dir: str = "models"
) -> Dict[str, Any]:
    """
    Main Prefect flow for heart disease ML training pipeline
    """
    logger = get_run_logger()
    logger.info("Starting Heart Disease ML Pipeline")
    logger.info(f"Processed data file: {processed_data_file}")
    logger.info(f"Test size: {test_size}")
    logger.info(f"Models directory: {models_dir}")
    
    # Load processed data
    df = load_processed_data(processed_data_file)
    
    # Prepare features and target
    X, y = prepare_features_and_target(df)
    
    # Split data
    X_train, X_test, y_train, y_test = split_data(X, y, test_size, random_state)
    
    # Train models
    lr_model, lr_info = train_logistic_regression(X_train, y_train)
    rf_model, rf_info = train_random_forest(X_train, y_train)
    
    # Evaluate models
    lr_metrics = evaluate_model(lr_model, X_test, y_test, "Logistic Regression")
    rf_metrics = evaluate_model(rf_model, X_test, y_test, "Random Forest")
    
    # Save models and metrics
    lr_model_path = save_model(lr_model, "Logistic Regression", lr_metrics, models_dir)
    rf_model_path = save_model(rf_model, "Random Forest", rf_metrics, models_dir)
    
    # Final summary
    pipeline_summary = {
        "timestamp": datetime.now().isoformat(),
        "data_shape": df.shape,
        "train_test_split": {
            "train_size": len(X_train),
            "test_size": len(X_test),
            "test_ratio": test_size
        },
        "models_trained": ["Logistic Regression", "Random Forest"],
        "model_paths": {
            "logistic_regression": lr_model_path,
            "random_forest": rf_model_path
        },
        "performance_summary": {
            "logistic_regression": {
                "accuracy": lr_metrics["accuracy"],
                "f1_score": lr_metrics["f1_score"],
                "cv_score": lr_info["cv_mean_score"]
            },
            "random_forest": {
                "accuracy": rf_metrics["accuracy"],
                "f1_score": rf_metrics["f1_score"],
                "cv_score": rf_info["cv_mean_score"]
            }
        },
        "status": "completed"
    }
    
    logger.info("ML Pipeline completed successfully!")
    logger.info(f"Summary: {pipeline_summary}")
    
    return pipeline_summary


if __name__ == "__main__":
    # Run the ML pipeline
    result = heart_disease_ml_pipeline()
    print(f"ML Pipeline completed: {result}")
