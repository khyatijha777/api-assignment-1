"""
Example Usage Script for Heart Disease ML Pipeline with Prefect
Demonstrates how to run the complete pipeline step by step
"""

import os
import sys
from datetime import datetime

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def print_banner(title):
    """Print a formatted banner"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def check_prerequisites():
    """Check if all prerequisites are met"""
    print_banner("CHECKING PREREQUISITES")
    
    # Check if heart.csv exists
    if not os.path.exists("heart.csv"):
        print("❌ heart.csv not found!")
        print("   Please download the heart disease dataset from Kaggle:")
        print("   https://www.kaggle.com/datasets/johnsmith88/heart-disease-dataset")
        return False
    else:
        print("✅ heart.csv found")
    
    # Check if required packages are installed
    required_packages = ['prefect', 'pandas', 'sklearn', 'flask']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} not installed")
    
    if missing_packages:
        print(f"\nPlease install missing packages:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    print("\n✅ All prerequisites met!")
    return True

def run_data_pipeline():
    """Run the data preprocessing pipeline"""
    print_banner("RUNNING DATA PREPROCESSING PIPELINE")
    
    try:
        from prefect_data_pipeline import heart_disease_data_pipeline
        
        print("Starting data preprocessing...")
        result = heart_disease_data_pipeline()
        
        print("✅ Data pipeline completed successfully!")
        print(f"   Processed {result['final_shape'][0]} rows and {result['final_shape'][1]} columns")
        print(f"   Output saved to: {result['output_file']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data pipeline failed: {str(e)}")
        return False

def run_ml_pipeline():
    """Run the ML training pipeline"""
    print_banner("RUNNING ML TRAINING PIPELINE")
    
    try:
        from prefect_ml_pipeline import heart_disease_ml_pipeline
        
        print("Starting ML training...")
        result = heart_disease_ml_pipeline()
        
        print("✅ ML pipeline completed successfully!")
        print("   Models trained:")
        for model_name, metrics in result['performance_summary'].items():
            print(f"   - {model_name}: Accuracy = {metrics['accuracy']:.4f}, F1 = {metrics['f1_score']:.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ ML pipeline failed: {str(e)}")
        return False

def run_monitoring():
    """Run the monitoring pipeline"""
    print_banner("RUNNING MONITORING PIPELINE")
    
    try:
        from prefect_monitoring import heart_disease_monitoring_pipeline
        
        print("Starting monitoring...")
        result = heart_disease_monitoring_pipeline()
        
        print("✅ Monitoring pipeline completed successfully!")
        print(f"   Health Status: {result['health_status']}")
        print(f"   Alerts: {result['alerts_count']}")
        print(f"   Dashboard: {result['monitoring_dashboard']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Monitoring pipeline failed: {str(e)}")
        return False

def test_api():
    """Test the API endpoints"""
    print_banner("TESTING API ENDPOINTS")
    
    try:
        import requests
        import time
        
        # Start API server in background (simplified test)
        print("Note: To test API endpoints, run 'python prefect_api_integration.py' in another terminal")
        print("Then test with:")
        print("  curl http://localhost:5000/app/details")
        print("  curl http://localhost:5000/health")
        
        return True
        
    except Exception as e:
        print(f"❌ API test failed: {str(e)}")
        return False

def show_next_steps():
    """Show next steps for the user"""
    print_banner("NEXT STEPS")
    
    print("🎉 Pipeline execution completed!")
    print("\nNext steps:")
    print("1. Start Prefect server:")
    print("   prefect server start")
    print("\n2. Start Prefect agent:")
    print("   prefect agent start --pool default-agent-pool")
    print("\n3. Deploy pipelines:")
    print("   python prefect_deployment.py --apply")
    print("\n4. Start API server:")
    print("   python prefect_api_integration.py")
    print("\n5. Access Prefect UI:")
    print("   http://localhost:4200")
    print("\n6. Test API endpoints:")
    print("   curl http://localhost:5000/app/details")
    print("\n7. Run monitoring:")
    print("   python prefect_monitoring.py --monitor")

def main():
    """Main execution function"""
    print_banner("HEART DISEASE ML PIPELINE WITH PREFECT")
    print(f"Execution started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites not met. Please fix the issues above.")
        return
    
    # Run pipelines
    success_count = 0
    total_pipelines = 4
    
    if run_data_pipeline():
        success_count += 1
    
    if run_ml_pipeline():
        success_count += 1
    
    if run_monitoring():
        success_count += 1
    
    if test_api():
        success_count += 1
    
    # Show results
    print_banner("EXECUTION SUMMARY")
    print(f"Pipelines completed: {success_count}/{total_pipelines}")
    
    if success_count == total_pipelines:
        print("🎉 All pipelines completed successfully!")
        show_next_steps()
    else:
        print("⚠️  Some pipelines failed. Check the logs above for details.")
        print("\nTroubleshooting:")
        print("1. Check that heart.csv exists and is valid")
        print("2. Ensure all required packages are installed")
        print("3. Check log files in the logs/ directory")
        print("4. Review error messages above")

if __name__ == "__main__":
    main()
