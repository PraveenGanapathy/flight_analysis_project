# scripts/download_flight_data.py
import os
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_flight_data():
    try:
        # Initialize the Kaggle API
        api = KaggleApi()
        api.authenticate()
        logger.info("Successfully authenticated with Kaggle")

        # Create data directory if it doesn't exist
        os.makedirs('data/raw', exist_ok=True)
        logger.info("Created data directory")

        # Dataset details - updated to correct dataset
        dataset = "rezashokrzad/flight-dataset"
        
        logger.info(f"Starting download of dataset: {dataset}")
        api.dataset_download_files(
            dataset,
            path='data/raw',
            unzip=True,
            quiet=False
        )
        logger.info("Download completed successfully")

        # Verify downloaded files
        expected_files = [
            'Combined_Flights_2018.parquet',
            'Combined_Flights_2019.parquet',
            'Combined_Flights_2020.parquet',
            'Combined_Flights_2021.parquet',
            'Combined_Flights_2022.parquet'
        ]

        downloaded_files = os.listdir('data/raw')
        for file in expected_files:
            if file in downloaded_files:
                file_size = os.path.getsize(os.path.join('data/raw', file))
                logger.info(f"Verified file: {file} (Size: {file_size/1024/1024:.2f} MB)")
            else:
                logger.warning(f"Missing file: {file}")

    except Exception as e:
        logger.error(f"Error during download: {str(e)}")
        raise

if __name__ == "__main__":
    download_flight_data()