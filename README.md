# Heart Disease Prediction ML Pipeline with Prefect

A comprehensive, production-ready machine learning pipeline for heart disease prediction using Prefect for workflow orchestration, monitoring, and deployment.

## 🎯 Overview

This project converts the original heart disease prediction pipeline into a robust, scalable ML pipeline using Prefect. It includes:

- **Data Preprocessing Pipeline**: Automated data cleaning, normalization, and encoding
- **ML Training Pipeline**: Model training with Logistic Regression and Random Forest
- **API Integration**: RESTful API endpoints for real-time monitoring
- **Scheduling & Deployment**: Automated scheduling with Prefect deployments
- **Monitoring & Alerting**: Comprehensive monitoring with health checks and alerts
- **Visual Dashboard**: Real-time monitoring dashboard

## 📁 Project Structure

```
heart_disease_prefect_pipeline/
│
├── prefect_data_pipeline.py          # Data preprocessing with Prefect flows
├── prefect_ml_pipeline.py            # ML training with Prefect flows
├── prefect_api_integration.py        # API integration with Prefect
├── prefect_deployment.py             # Scheduling and deployment configuration
├── prefect_monitoring.py             # Monitoring and alerting system
├── requirements.txt                  # Python dependencies
├── README.md                         # This file
│
├── data/                            # Data directory
│   ├── heart.csv                    # Original dataset (download from Kaggle)
│   └── heart_processed.csv          # Processed dataset (generated)
│
├── models/                          # Trained models directory
│   ├── logistic_regression_model.pkl
│   ├── random_forest_model.pkl
│   ├── logistic_regression_metrics.json
│   └── random_forest_metrics.json
│
├── logs/                            # Log files directory
│   ├── heart_disease_pipeline.log
│   ├── data_pipeline.log
│   ├── ml_pipeline.log
│   ├── api.log
│   └── monitoring.log
│
├── reports/                         # Performance reports
├── dashboard/                       # Monitoring dashboards
└── alerts/                         # Alert files
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Install required packages
pip install prefect pandas scikit-learn flask seaborn matplotlib joblib

# Or install from requirements.txt
pip install -r requirements.txt
```

### 2. Download Dataset

Download the heart disease dataset from Kaggle:
- URL: https://www.kaggle.com/datasets/johnsmith88/heart-disease-dataset
- Save as `heart.csv` in the project root

### 3. Initialize Prefect

```bash
# Start Prefect server
prefect server start

# In another terminal, start a work pool agent
prefect agent start --pool default-agent-pool
```

### 4. Deploy Pipelines

```bash
# Create and apply deployments
python prefect_deployment.py --apply
```

### 5. Start API Server

```bash
# Start the API server
python prefect_api_integration.py
```

### 6. Initialize Monitoring

```bash
# Initialize monitoring system
python prefect_monitoring.py --init

# Run monitoring pipeline
python prefect_monitoring.py --monitor
```

## 📊 Pipeline Components

### 1. Data Preprocessing Pipeline (`prefect_data_pipeline.py`)

**Features:**
- Automated data loading and validation
- Missing value handling
- Numerical feature normalization
- Categorical feature encoding
- Comprehensive logging and error handling

**Tasks:**
- `load_heart_data`: Load dataset from CSV
- `check_data_quality`: Validate data quality
- `handle_missing_values`: Clean missing data
- `normalize_numerical_features`: Scale numerical features
- `encode_categorical_features`: One-hot encode categorical features
- `save_processed_data`: Save processed dataset

**Flow:** `heart-disease-data-pipeline`

### 2. ML Training Pipeline (`prefect_ml_pipeline.py`)

**Features:**
- Automated train-test splitting
- Multiple model training (Logistic Regression, Random Forest)
- Cross-validation scoring
- Comprehensive model evaluation
- Model persistence and metrics saving

**Tasks:**
- `load_processed_data`: Load processed dataset
- `prepare_features_and_target`: Prepare X and y
- `split_data`: Train-test split
- `train_logistic_regression`: Train LR model
- `train_random_forest`: Train RF model
- `evaluate_model`: Evaluate model performance
- `save_model`: Save trained models and metrics

**Flow:** `heart-disease-ml-pipeline`

### 3. API Integration (`prefect_api_integration.py`)

**Features:**
- RESTful API endpoints
- Real-time pipeline status monitoring
- Model information retrieval
- Dataset statistics
- Health check endpoints

**Endpoints:**
- `GET /app/details`: Main application details
- `GET /pipeline/status`: Detailed pipeline status
- `GET /models/info`: Model information
- `GET /dataset/info`: Dataset information
- `GET /logs/recent`: Recent log entries
- `GET /health`: Health check

**Flows:** `api-status-pipeline`

### 4. Deployment & Scheduling (`prefect_deployment.py`)

**Features:**
- Automated scheduling with Cron and Interval schedules
- Multiple deployment configurations
- Manual and scheduled execution options
- Work pool management

**Deployments:**
- Data Pipeline: Every 2 minutes
- ML Pipeline: Daily at 6 AM
- Complete Orchestrator: Every 4 hours
- API Status: Every 5 minutes

### 5. Monitoring & Alerting (`prefect_monitoring.py`)

**Features:**
- Comprehensive metrics collection
- Performance reporting
- Health checking
- Alert system
- Visual dashboard generation

**Tasks:**
- `collect_pipeline_metrics`: Gather system metrics
- `generate_performance_report`: Create performance reports
- `create_monitoring_dashboard`: Generate visual dashboards
- `check_pipeline_health`: Health check all components
- `send_alerts`: Process and send alerts

**Flow:** `heart-disease-monitoring-pipeline`

## 🔧 Configuration

### Environment Variables

Create a `.env` file for configuration:

```env
# Prefect Configuration
PREFECT_API_URL=http://localhost:4200/api
PREFECT_API_KEY=your_api_key_here

# Pipeline Configuration
DATA_FILE=heart.csv
PROCESSED_FILE=heart_processed.csv
MODELS_DIR=models
TEST_SIZE=0.3
RANDOM_STATE=42

# API Configuration
API_HOST=0.0.0.0
API_PORT=5000
API_DEBUG=True

# Monitoring Configuration
LOG_LEVEL=INFO
ALERT_EMAIL=your_email@example.com
```

### Scheduling Configuration

Modify `prefect_deployment.py` to adjust scheduling:

```python
# Data pipeline - every 2 minutes
schedule=IntervalSchedule(interval=timedelta(minutes=2))

# ML pipeline - daily at 6 AM
schedule=CronSchedule(cron="0 6 * * *")

# Complete orchestrator - every 4 hours
schedule=IntervalSchedule(interval=timedelta(hours=4))
```

## 📈 Usage Examples

### Running Individual Pipelines

```python
# Run data preprocessing
from prefect_data_pipeline import heart_disease_data_pipeline
result = heart_disease_data_pipeline()

# Run ML training
from prefect_ml_pipeline import heart_disease_ml_pipeline
result = heart_disease_ml_pipeline()

# Run monitoring
from prefect_monitoring import heart_disease_monitoring_pipeline
result = heart_disease_monitoring_pipeline()
```

### API Usage

```bash
# Get application details
curl http://localhost:5000/app/details

# Get pipeline status
curl http://localhost:5000/pipeline/status

# Get model information
curl http://localhost:5000/models/info

# Health check
curl http://localhost:5000/health
```

### Prefect CLI Commands

```bash
# View flows
prefect flow ls

# View deployments
prefect deployment ls

# Run a deployment manually
prefect deployment run "heart-disease-data-pipeline-deployment"

# View flow runs
prefect flow-run ls

# View logs
prefect flow-run logs <flow-run-id>
```

## 📊 Monitoring & Observability

### Prefect UI

Access the Prefect UI at `http://localhost:4200` to:
- Monitor flow runs
- View deployment status
- Check logs and metrics
- Manage schedules

### Log Files

Monitor log files in the `logs/` directory:
- `heart_disease_pipeline.log`: Main pipeline logs
- `data_pipeline.log`: Data preprocessing logs
- `ml_pipeline.log`: ML training logs
- `api.log`: API request logs
- `monitoring.log`: Monitoring system logs

### Performance Reports

Generated reports in `reports/` directory include:
- Pipeline performance metrics
- Model accuracy scores
- System resource usage
- Recommendations for optimization

### Visual Dashboards

Generated dashboards in `dashboard/` directory show:
- Pipeline status overview
- Model performance metrics
- System resource usage
- Component health status

## 🚨 Alerting

The monitoring system generates alerts for:
- Pipeline failures
- Data quality issues
- Model performance degradation
- System resource constraints
- Stale data

Alerts are saved to the `alerts/` directory and can be integrated with:
- Email notifications
- Slack webhooks
- PagerDuty
- Custom webhook endpoints

## 🔍 Troubleshooting

### Common Issues

1. **Prefect Server Not Starting**
   ```bash
   # Check if port 4200 is available
   netstat -an | grep 4200
   
   # Start with different port
   prefect server start --host 0.0.0.0 --port 4201
   ```

2. **Dataset Not Found**
   ```bash
   # Ensure heart.csv is in the project root
   ls -la heart.csv
   
   # Download from Kaggle if missing
   ```

3. **Model Training Fails**
   ```bash
   # Check if processed data exists
   ls -la heart_processed.csv
   
   # Run data pipeline first
   python prefect_data_pipeline.py
   ```

4. **API Not Responding**
   ```bash
   # Check if API server is running
   curl http://localhost:5000/health
   
   # Check logs for errors
   tail -f logs/api.log
   ```

### Debug Mode

Enable debug mode for detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🧪 Testing

### Unit Tests

```bash
# Run individual pipeline tests
python -m pytest tests/test_data_pipeline.py
python -m pytest tests/test_ml_pipeline.py
python -m pytest tests/test_api.py
```

### Integration Tests

```bash
# Run full pipeline test
python -m pytest tests/test_integration.py
```

### Load Testing

```bash
# Test API endpoints under load
python tests/load_test_api.py
```

## 📚 Advanced Features

### Custom Task Decorators

```python
from prefect import task
from prefect.task_runners import ConcurrentTaskRunner

@task(retries=3, retry_delay_seconds=5)
def custom_task():
    # Your custom logic
    pass
```

### Work Pools

```bash
# Create custom work pool
prefect work-pool create --type process custom-pool

# Start agent for custom pool
prefect agent start --pool custom-pool
```

### Secrets Management

```python
from prefect.blocks.system import Secret

# Store secret
secret = Secret(value="your-secret-value")
secret.save(name="api-key")

# Use secret
api_key = Secret.load("api-key").get()
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Original heart disease dataset from Kaggle
- Prefect team for the excellent workflow orchestration framework
- Scikit-learn for machine learning tools
- Flask for API framework

## 📞 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review Prefect documentation: https://docs.prefect.io/

---

**Happy Pipeline Building! 🚀**
