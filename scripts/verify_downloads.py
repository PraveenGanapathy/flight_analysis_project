import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_downloads():
    data_dir = Path('data/raw')
    
    # Expected files and minimum sizes (in MB)
    expected_files = {
        'Combined_Flights_2018.parquet': 200,  # Approximate size
        'Combined_Flights_2019.parquet': 200,
        'Combined_Flights_2020.parquet': 150,
        'Combined_Flights_2021.parquet': 200,
        'Combined_Flights_2022.parquet': 140
    }
    
    for filename, min_size in expected_files.items():
        file_path = data_dir / filename
        
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            
            # Check file size
            if size_mb < min_size:
                logger.warning(f"{filename}: File size ({size_mb:.2f} MB) is smaller than expected")
                continue
                
            # Try reading the file
            try:
                df = pd.read_parquet(file_path)
                logger.info(f"✓ {filename}:")
                logger.info(f"  - Size: {size_mb:.2f} MB")
                logger.info(f"  - Rows: {len(df):,}")
                logger.info(f"  - Columns: {df.columns.tolist()}")
            except Exception as e:
                logger.error(f"Error reading {filename}: {str(e)}")
        else:
            logger.error(f"❌ Missing file: {filename}")

if __name__ == "__main__":
    verify_downloads()