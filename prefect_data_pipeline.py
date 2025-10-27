"""
Prefect Data Pipeline for Heart Disease Prediction
Converts the original data pipeline to use Prefect flows and tasks
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from prefect import flow, task, get_run_logger
from typing import Tuple, Dict, Any
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import json


@task(name="load_heart_data", retries=2, retry_delay_seconds=5)
def load_heart_data(file_path: str = "heart.csv") -> pd.DataFrame:
    """
    Load the heart disease dataset from CSV file
    """
    logger = get_run_logger()
    logger.info(f"Loading data from {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded data with shape: {df.shape}")
        logger.info(f"Columns: {list(df.columns)}")
        return df
    except FileNotFoundError:
        logger.error(f"File {file_path} not found")
        raise
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise


@task(name="check_data_quality", retries=1)
def check_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Check data quality and return summary statistics
    """
    logger = get_run_logger()
    
    quality_report = {
        "shape": df.shape,
        "missing_values": df.isnull().sum().to_dict(),
        "data_types": df.dtypes.to_dict(),
        "duplicate_rows": df.duplicated().sum(),
        "memory_usage": df.memory_usage(deep=True).sum()
    }
    
    logger.info(f"Data Quality Report:")
    logger.info(f"Shape: {quality_report['shape']}")
    logger.info(f"Missing values: {quality_report['missing_values']}")
    logger.info(f"Duplicate rows: {quality_report['duplicate_rows']}")
    
    return quality_report


@task(name="handle_missing_values", retries=1)
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values in the dataset
    """
    logger = get_run_logger()
    
    missing_before = df.isnull().sum().sum()
    logger.info(f"Missing values before handling: {missing_before}")
    
    if missing_before > 0:
        # Fill numeric columns with mean
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
        
        # Fill categorical columns with mode
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'Unknown')
        
        missing_after = df.isnull().sum().sum()
        logger.info(f"Missing values after handling: {missing_after}")
    else:
        logger.info("No missing values found")
    
    return df


@task(name="normalize_numerical_features", retries=1)
def normalize_numerical_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, MinMaxScaler]:
    """
    Normalize numerical features using MinMaxScaler
    """
    logger = get_run_logger()
    
    # Define numerical columns to normalize
    numerical_cols = ['age', 'trestbps', 'chol', 'thalach', 'oldpeak']
    
    # Check which columns exist in the dataset
    existing_numerical_cols = [col for col in numerical_cols if col in df.columns]
    logger.info(f"Normalizing columns: {existing_numerical_cols}")
    
    if existing_numerical_cols:
        scaler = MinMaxScaler()
        df[existing_numerical_cols] = scaler.fit_transform(df[existing_numerical_cols])
        logger.info(f"Successfully normalized {len(existing_numerical_cols)} numerical columns")
    else:
        scaler = MinMaxScaler()
        logger.warning("No numerical columns found for normalization")
    
    return df, scaler


@task(name="encode_categorical_features", retries=1)
def encode_categorical_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Encode categorical features using one-hot encoding
    """
    logger = get_run_logger()
    
    # Define categorical columns to encode
    categorical_cols = ['sex', 'cp', 'fbs', 'restecg', 'exang', 'slope', 'ca', 'thal']
    
    # Check which columns exist in the dataset
    existing_categorical_cols = [col for col in categorical_cols if col in df.columns]
    logger.info(f"Encoding categorical columns: {existing_categorical_cols}")
    
    if existing_categorical_cols:
        df_encoded = pd.get_dummies(df, columns=existing_categorical_cols, drop_first=True)
        logger.info(f"Successfully encoded {len(existing_categorical_cols)} categorical columns")
        logger.info(f"Shape after encoding: {df_encoded.shape}")
        return df_encoded
    else:
        logger.warning("No categorical columns found for encoding")
        return df


@task(name="calculate_correlations", retries=1)
def calculate_correlations(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate correlation coefficients for numerical features
    """
    logger = get_run_logger()
    
    # Select only numerical columns
    numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    if len(numerical_cols) > 0:
        # Calculate correlation matrix
        correlation_matrix = df[numerical_cols].corr()
        
        # Get correlations with target if it exists
        correlation_report = {
            "correlation_matrix": correlation_matrix.to_dict(),
            "feature_target_correlations": {},
            "top_correlations": {}
        }
        
        # Check if target column exists
        if 'target' in df.columns:
            target_correlations = df[numerical_cols].corrwith(df['target']).to_dict()
            correlation_report["feature_target_correlations"] = target_correlations
            
            # Sort correlations with target
            sorted_correlations = sorted(target_correlations.items(), 
                                        key=lambda x: abs(x[1]), reverse=True)
            correlation_report["top_correlations"] = dict(sorted_correlations[:5])
            
            logger.info("Top 5 correlations with target:")
            for feature, corr in correlation_report["top_correlations"].items():
                logger.info(f"  {feature}: {corr:.4f}")
        
        # Find highly correlated feature pairs (for multicollinearity detection)
        high_correlations = {}
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                corr_val = correlation_matrix.iloc[i, j]
                if abs(corr_val) > 0.7:  # Threshold for high correlation
                    high_correlations[f"{correlation_matrix.columns[i]}-{correlation_matrix.columns[j]}"] = corr_val
        
        if high_correlations:
            logger.warning(f"High correlations detected: {high_correlations}")
            correlation_report["high_correlations"] = high_correlations
        
        logger.info("Correlation analysis completed")
        return correlation_report
    else:
        logger.warning("No numerical columns found for correlation analysis")
        return {"error": "No numerical columns"}


@task(name="create_eda_visualizations", retries=1)
def create_eda_visualizations(df: pd.DataFrame, output_dir: str = "dashboard") -> Dict[str, str]:
    """
    Create exploratory data analysis visualizations
    """
    logger = get_run_logger()
    logger.info("Creating EDA visualizations")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    visualization_files = {}
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Select numerical columns for visualization
    numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    if len(numerical_cols) == 0:
        logger.warning("No numerical columns for visualization")
        return {}
    
    try: 
        # 1. Correlation Heatmap
        plt.figure(figsize=(12, 10))
        correlation_matrix = df[numerical_cols].corr()
        sns.heatmap(correlation_matrix, annot=True, fmt='.2f', cmap='coolwarm', 
                   center=0, square=True, linewidths=0.5, cbar_kws={"shrink": 0.8})
        plt.title('Feature Correlation Heatmap', fontsize=16, pad=20)
        plt.tight_layout()
        
        corr_file = os.path.join(output_dir, f'correlation_heatmap_{timestamp}.png')
        plt.savefig(corr_file, dpi=300, bbox_inches='tight')
        plt.close()
        visualization_files['correlation_heatmap'] = corr_file
        logger.info(f"Correlation heatmap saved: {corr_file}")
        
        # 2. Target Distribution
        if 'target' in df.columns:
            plt.figure(figsize=(8, 6))
            df['target'].value_counts().plot(kind='bar', color=['skyblue', 'salmon'])
            plt.title('Target Variable Distribution', fontsize=14)
            plt.xlabel('Heart Disease (0=No, 1=Yes)')
            plt.ylabel('Count')
            plt.xticks(rotation=0)
            plt.tight_layout()
            
            target_file = os.path.join(output_dir, f'target_distribution_{timestamp}.png')
            plt.savefig(target_file, dpi=300, bbox_inches='tight')
            plt.close()
            visualization_files['target_distribution'] = target_file
            logger.info(f"Target distribution saved: {target_file}")
            
            # 3. Feature distributions for positive vs negative cases
            if len(numerical_cols) > 0:
                # Select top 6 features for visualization
                top_features = numerical_cols[:6]
                
                fig, axes = plt.subplots(2, 3, figsize=(18, 12))
                fig.suptitle('Feature Distributions by Target Variable', fontsize=16)
                
                for idx, feature in enumerate(top_features):
                    if feature != 'target':
                        ax = axes[idx // 3, idx % 3]
                        df.boxplot(column=feature, by='target', ax=ax)
                        ax.set_title(f'{feature} by Target')
                        ax.set_xlabel('Target')
                        ax.set_ylabel(feature)
                
                plt.tight_layout()
                box_file = os.path.join(output_dir, f'feature_boxplots_{timestamp}.png')
                plt.savefig(box_file, dpi=300, bbox_inches='tight')
                plt.close()
                visualization_files['feature_boxplots'] = box_file
                logger.info(f"Feature boxplots saved: {box_file}")
        
        logger.info(f"EDA visualizations completed: {len(visualization_files)} files created")
        return visualization_files
        
    except Exception as e:
        logger.error(f"Error creating visualizations: {str(e)}")
        return {}


@task(name="save_processed_data", retries=2)
def save_processed_data(df: pd.DataFrame, output_path: str = "heart_processed.csv") -> str:
    """
    Save the processed dataset to CSV file
    """
    logger = get_run_logger()
    
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Successfully saved processed data to {output_path}")
        logger.info(f"Final dataset shape: {df.shape}")
        
        # Log file size
        file_size = os.path.getsize(output_path)
        logger.info(f"File size: {file_size} bytes")
        
        return output_path
    except Exception as e:
        logger.error(f"Error saving processed data: {str(e)}")
        raise


@flow(
    name="heart-disease-data-pipeline",
    description="Complete data preprocessing pipeline for heart disease prediction"
)
def heart_disease_data_pipeline(
    input_file: str = "heart.csv",
    output_file: str = "heart_processed.csv"
) -> Dict[str, Any]:
    """
    Main Prefect flow for heart disease data preprocessing pipeline
    """
    logger = get_run_logger()
    logger.info("Starting Heart Disease Data Pipeline")
    logger.info(f"Input file: {input_file}")
    logger.info(f"Output file: {output_file}")
    
    # Load data
    df = load_heart_data(input_file)
    
    # Check data quality
    quality_report = check_data_quality(df)
    
    # Handle missing values
    df_clean = handle_missing_values(df)
    
    # Normalize numerical features
    df_normalized, scaler = normalize_numerical_features(df_clean)
    
    # Encode categorical features
    df_processed = encode_categorical_features(df_normalized)
    
    # Calculate correlations (EDA)
    correlation_report = calculate_correlations(df_processed)
    
    # Create EDA visualizations
    visualizations = create_eda_visualizations(df_processed)
    
    # Save processed data
    output_path = save_processed_data(df_processed, output_file)
    
    # Save correlation report
    correlation_report_path = "correlation_report.json"
    with open(correlation_report_path, 'w') as f:
        json.dump(correlation_report, f, indent=2, default=str)
    logger.info(f"Correlation report saved: {correlation_report_path}")
    
    # Final summary
    pipeline_summary = {
        "input_file": input_file,
        "output_file": output_path,
        "original_shape": quality_report["shape"],
        "final_shape": df_processed.shape,
        "missing_values_handled": quality_report["missing_values"],
        "correlation_report": correlation_report_path,
        "visualizations": list(visualizations.keys()),
        "visualization_files": visualizations,
        "timestamp": datetime.now().isoformat(),
        "status": "completed"
    }
    
    logger.info("Pipeline completed successfully!")
    logger.info(f"Summary: {pipeline_summary}")
    
    return pipeline_summary


if __name__ == "__main__":
    # Run the pipeline
    result = heart_disease_data_pipeline()
    print(f"Pipeline completed: {result}")
