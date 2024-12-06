from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
import logging
from pathlib import Path
import boto3
import pandas as pd
import io
import papermill as pm
import subprocess
from sagemaker import get_execution_role, Session, ScriptProcessor, ProcessingInput, ProcessingOutput
import shutil
from airflow.utils.dates import days_ago
import requests


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define constants
LOCAL_RAW_FOLDER = "/Users/Praveen/bd_project/flight_analysis_project/data/raw"
NOTEBOOK_PATH = "/Users/Praveen/bd_project/flight_analysis_project/notebooks/flight_analysis.ipynb"
PRESENTATION_FOLDER = "/Users/Praveen/bd_project/flight_analysis_project/data/presentation"
PROCESSED_FOLDER = "/Users/Praveen/bd_project/flight_analysis_project/data/processed"
DASHBOARD_PATH = "/Users/Praveen/bd_project/flight_analysis_project/dashboard/flight_dashboard.py"
S3_BUCKET_NAME = "flight-delay-analytics"
S3_FOLDERS = ["raw", "processed", "presentation"]
S3_RAW_FOLDER = 'raw'
S3_PROCESSED_FOLDER = 'processed'
S3_PRESENTATION_FOLDER = 'presentation'
STREAMLIT_PORT = 8502  # Specify a fixed port
STREAMLIT_URL = f"http://localhost:{STREAMLIT_PORT}"

def check_local_files(**context):
    """Ensure files exist in the local raw folder."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(LOCAL_RAW_FOLDER, exist_ok=True)
        
        parquet_files = list(Path(LOCAL_RAW_FOLDER).glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {LOCAL_RAW_FOLDER}")
        
        logger.info(f"Found {len(parquet_files)} parquet files")
        return [str(f) for f in parquet_files]
        
    except Exception as e:
        logger.error(f"Error checking files: {str(e)}")
        raise
def remove_local_folders(**context):
    """Remove the presentation folder and all files in the processed folder."""
    try:
        # Check and remove the 'presentation' folder
        if os.path.exists(PRESENTATION_FOLDER):
            shutil.rmtree(PRESENTATION_FOLDER)  # Removes the folder and all its contents
            logger.info(f"Removed local presentation folder: {PRESENTATION_FOLDER}")
        else:
            logger.info(f"Presentation folder does not exist: {PRESENTATION_FOLDER}")

        # Check and remove the 'processed' folder
        if os.path.exists(PROCESSED_FOLDER):
            shutil.rmtree(PROCESSED_FOLDER)  # Removes the folder and all its contents
            logger.info(f"Removed local processed folder: {PROCESSED_FOLDER}")
        else:
            logger.info(f"Processed folder does not exist: {PROCESSED_FOLDER}")

    except Exception as e:
        logger.error(f"Error removing folders: {str(e)}")
        raise

def check_and_create_s3_folders(**context):
    """Ensure S3 bucket and folders exist"""
    try:
        s3_hook = S3Hook(aws_conn_id='aws_default', region_name='us-east-1')
        bucket_exists = s3_hook.check_for_bucket('flight-delay-analytics')

        if not bucket_exists:
            logger.info("Creating new bucket")
            s3_hook.create_bucket(bucket_name='flight-delay-analytics', region_name='us-east-1')

        for folder in S3_FOLDERS:
            prefix = f"{folder}/"
            s3_hook.load_string(
                string_data='',
                key=prefix,
                bucket_name='flight-delay-analytics',
                replace=True
            )
            logger.info(f"Created folder: {prefix}")

        return "S3 setup completed"

    except Exception as e:
        logger.error(f"Error in S3 setup: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        raise


def upload_parquet_to_s3(**context):
    """Upload all parquet files from the local raw folder to S3, skipping already existing ones."""
    try:
        # Initialize S3 hook
        s3_hook = S3Hook(aws_conn_id='aws_default', region_name='us-east-1')

        # Check if the bucket exists
        bucket_exists = s3_hook.check_for_bucket(S3_BUCKET_NAME)
        if not bucket_exists:
            logger.info(f"Bucket {S3_BUCKET_NAME} does not exist.")
            raise Exception(f"Bucket {S3_BUCKET_NAME} not found.")

        # List all parquet files in the LOCAL_RAW_FOLDER
        parquet_files = [f for f in os.listdir(LOCAL_RAW_FOLDER) if f.endswith('.parquet')]

        if not parquet_files:
            logger.info(f"No parquet files found in local folder: {LOCAL_RAW_FOLDER}")
            return "No parquet files to upload."

        # Upload each parquet file to the S3 bucket
        for file_name in parquet_files:
            local_file_path = os.path.join(LOCAL_RAW_FOLDER, file_name)
            s3_key = f"{S3_RAW_FOLDER}/{file_name}"

            # Check if the file already exists in S3
            if s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET_NAME):
                logger.info(f"File {file_name} already exists in S3 bucket {S3_BUCKET_NAME}. Skipping upload.")
                continue

            # Upload the file
            s3_hook.load_file(
                filename=local_file_path,
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True  # Set to False if you want to append, or keep existing files
            )

            logger.info(f"Uploaded {file_name} to S3 bucket {S3_BUCKET_NAME} under {S3_RAW_FOLDER}/")

        return "Parquet files uploaded to S3, skipping already existing files."

    except Exception as e:
        logger.error(f"Error uploading parquet files: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        raise



def run_notebook(**context):
    """Run the Jupyter notebook using Papermill."""
    try:
        def ensure_directory_exists(directory):
            if not os.path.exists(directory):
                os.makedirs(directory)
                subprocess.run(["chmod", "-R", "755", directory], check=True)

        # Before executing Papermill
        ensure_directory_exists("/Users/Praveen/bd_project/flight_analysis_project/data/processed")
        output_path = "/Users/Praveen/bd_project/flight_analysis_project/data/processed/flight_analysis_op.ipynb"
        pm.execute_notebook(
            NOTEBOOK_PATH,
            output_path
        )
        logger.info(f"Notebook executed successfully. Output saved at {output_path}")
    except Exception as e:
        logger.error(f"Error running the notebook: {str(e)}")
        raise
# Initialize the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def save_presentation_results(**context):
    """Upload all files from local processed and presentation folders to S3, 
    replacing existing files if they already exist in the respective folders."""
    try:
        # Initialize the S3 hook
        s3_hook = S3Hook(aws_conn_id='aws_default', region_name='us-east-1')

        # Check if the bucket exists
        bucket_exists = s3_hook.check_for_bucket(S3_BUCKET_NAME)
        if not bucket_exists:
            logger.info(f"Bucket {S3_BUCKET_NAME} does not exist.")
            raise Exception(f"Bucket {S3_BUCKET_NAME} not found.")

        # List of local folders to upload from
        folders_to_upload = [
            (PROCESSED_FOLDER, S3_PROCESSED_FOLDER),
            (PRESENTATION_FOLDER, S3_PRESENTATION_FOLDER)
        ]

        for local_folder, s3_folder in folders_to_upload:
            # List all files in the local folder
            files = [f for f in os.listdir(local_folder) if os.path.isfile(os.path.join(local_folder, f))]

            if not files:
                logger.info(f"No files found in local folder: {local_folder}")
                continue

            # Upload each file to the respective S3 folder
            for file_name in files:
                local_file_path = os.path.join(local_folder, file_name)
                s3_key = f"{s3_folder}/{file_name}"

                # Check if the file exists in the S3 bucket
                if s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET_NAME):
                    # Delete the existing file in S3 if it exists
                    s3_hook.delete_objects(S3_BUCKET_NAME, keys=[s3_key])
                    #s3_hook.delete_objects(bucket_name=S3_BUCKET_NAME, keys=[s3_key])
                    logger.info(f"Deleted existing file {file_name} from S3 bucket {S3_BUCKET_NAME} under {s3_folder}/")

                # Upload the file to S3
                s3_hook.load_file(
                    filename=local_file_path,
                    key=s3_key,
                    bucket_name=S3_BUCKET_NAME,
                    replace=True  # Ensure to overwrite the file if it exists
                )

                logger.info(f"Uploaded {file_name} to S3 bucket {S3_BUCKET_NAME} under {s3_folder}/")

        return "Files uploaded successfully to S3, existing files replaced."

    except Exception as e:
        logger.error(f"Error uploading presentation results: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        raise

def check_streamlit_running():
    """Check if the Streamlit server is running."""
    try:
        response = requests.get(STREAMLIT_URL, timeout=5)
        if response.status_code == 200:
            return True
    except requests.ConnectionError:
        raise Exception("Streamlit server is not running!")
    raise Exception("Streamlit server is running but returned a non-200 status.")



# Define DAG
with DAG(
    "flight_analysis_workflow",
    default_args={"retries": 1},
    description="Flight analysis ETL workflow",
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    check_files_task = PythonOperator(
        task_id="check_local_files",
        python_callable=check_local_files
    )

    remove_local_folders_task = PythonOperator(
        task_id="remove_local_folders",
        python_callable=remove_local_folders
    )

    check_s3_task = PythonOperator(
        task_id="check_and_create_s3_folders",
        python_callable=check_and_create_s3_folders
    )

    upload_parquet_to_s3_task = PythonOperator(
        task_id="upload_parquet_to_s3",
        python_callable=upload_parquet_to_s3
    )

    run_notebook_task = PythonOperator(
        task_id="run_notebook",
        python_callable=run_notebook
    )


    save_results_task = PythonOperator(
        task_id="save_presentation_results",
        python_callable=save_presentation_results
    )


    trigger_streamlit_task = BashOperator(
        task_id="start_streamlit",
        bash_command=f"""
        if [ -f "/Users/Praveen/airflow/airflow-webserver.pid" ]; then
            echo "Cleaning up stale PID file...";
            rm /Users/Praveen/airflow/airflow-webserver.pid;
        fi
        streamlit run {DASHBOARD_PATH} --server.port {STREAMLIT_PORT} &
        sleep 10  # Allow time for the server to start
        """
    )

    # Step 2: Check if Streamlit is Running
    verify_streamlit_task = PythonOperator(
        task_id="verify_streamlit",
        python_callable=check_streamlit_running,
    )


    # Define task dependencies
    start >> check_files_task >> remove_local_folders_task >> check_s3_task >> upload_parquet_to_s3_task >> run_notebook_task >> save_results_task >> trigger_streamlit_task >> verify_streamlit_task >>end