"""
Prefect Scheduling and Deployment Configuration
Sets up automated scheduling and deployment for the heart disease ML pipeline
"""

from prefect import flow, serve
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule
from datetime import timedelta
import os

# Import our pipeline flows
from prefect_data_pipeline import heart_disease_data_pipeline
from prefect_ml_pipeline import heart_disease_ml_pipeline
from prefect_api_integration import api_status_pipeline, update_pipeline_status


@flow(name="heart-disease-orchestrator")
def heart_disease_orchestrator():
    """
    Orchestrator flow that runs the complete pipeline
    """
    print("Starting Heart Disease ML Pipeline Orchestrator")
    
    try:
        # Update status
        update_pipeline_status("data_pipeline", "running")
        
        # Run data pipeline
        print("Running data preprocessing pipeline...")
        data_result = heart_disease_data_pipeline()
        update_pipeline_status("data_pipeline", "completed", data_result)
        
        # Update status
        update_pipeline_status("ml_pipeline", "running")
        
        # Run ML pipeline
        print("Running ML training pipeline...")
        ml_result = heart_disease_ml_pipeline()
        update_pipeline_status("ml_pipeline", "completed", ml_result)
        
        print("Complete pipeline finished successfully!")
        return {
            "data_pipeline": data_result,
            "ml_pipeline": ml_result,
            "status": "completed"
        }
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        update_pipeline_status("data_pipeline", "failed", {"error": str(e)})
        update_pipeline_status("ml_pipeline", "failed", {"error": str(e)})
        raise


# For Prefect 3.x, we use serve() to deploy flows with schedules
# The flows will be deployed when you run: python prefect_deployment.py

def deploy_with_schedules():
    """
    Deploy flows with schedules using Prefect 3.x serve() method
    This will make the flows available on Prefect server
    """
    print("Setting up Prefect 3.x deployments...")
    print("Note: This will start serving the flows on the Prefect server.")
    print("Make sure 'prefect server start' is running first!\n")
    
    # Start serving the data pipeline with schedule
    print("Deploying data pipeline with 2-minute interval...")
    heart_disease_data_pipeline.serve(
        name="heart-disease-data-pipeline",
        tags=["data", "preprocessing", "heart-disease"],
        parameters={"input_file": "heart.csv", "output_file": "heart_processed.csv"},
        cron="*/2 * * * *"  # Every 2 minutes
    )


def run_flows_directly():
    """
    Run flows directly without deployment
    Use this for manual execution or testing
    """
    print("Running flows directly...")
    
    print("\n1. Running Data Pipeline...")
    try:
        result = heart_disease_data_pipeline()
        print(f"Data Pipeline Result: {result}")
    except Exception as e:
        print(f"Data Pipeline Error: {e}")
    
    print("\n2. Running ML Pipeline...")
    try:
        result = heart_disease_ml_pipeline()
        print(f"ML Pipeline Result: {result}")
    except Exception as e:
        print(f"ML Pipeline Error: {e}")
    
    print("\n3. Running Orchestrator...")
    try:
        result = heart_disease_orchestrator()
        print(f"Orchestrator Result: {result}")
    except Exception as e:
        print(f"Orchestrator Error: {e}")


def setup_prefect_server():
    """
    Setup instructions for Prefect server
    """
    print("""
    ========================================
    Prefect Server Setup Instructions
    ========================================
    
    1. Start Prefect Server:
       prefect server start
    
    2. In another terminal, start a work pool agent:
       prefect agent start --pool default-agent-pool
    
    3. Apply deployments:
       python prefect_deployment.py --apply
    
    4. View Prefect UI:
       Open http://localhost:4200 in your browser
    
    5. Start API server:
       python prefect_api_integration.py
    
    ========================================
    """)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Prefect Deployment Manager")
    parser.add_argument("--run", action="store_true", help="Run flows directly without deployment")
    parser.add_argument("--setup", action="store_true", help="Show setup instructions")
    parser.add_argument("--deploy", action="store_true", help="Deploy flows with schedules")
    
    args = parser.parse_args()
    
    if args.setup:
        setup_prefect_server()
    elif args.deploy:
        deploy_with_schedules()
    elif args.run:
        run_flows_directly()
    else:
        print("""
        Prefect Deployment Manager for Heart Disease ML Pipeline (Prefect 3.x)
        
        Usage:
        python prefect_deployment.py --run       # Run flows directly without deployment
        python prefect_deployment.py --deploy   # Deploy flows with schedules (requires server running)
        python prefect_deployment.py --setup    # Show setup instructions
        
        Important Setup Steps:
        1. Start Prefect server: prefect server start
        2. Start the flows: python prefect_deployment.py --deploy
        3. View Prefect UI: Open http://localhost:4200
        
        Available Flows:
        1. Data Pipeline - Runs every 2 minutes
        2. ML Pipeline - Manual or scheduled execution
        3. Complete Orchestrator - Runs all pipelines
        """)
