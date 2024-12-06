#!/bin/bash

# Install required packages
pip install kaggle boto3 pandas

# Create Kaggle credentials directory
mkdir -p ~/.kaggle

# Note: You need to manually place your kaggle.json file in ~/.kaggle/
echo "Please place your kaggle.json file in ~/.kaggle/"
echo "You can download this from Kaggle → Account → Create API Token"

# Set permissions for Kaggle API key
chmod 600 ~/.kaggle/kaggle.json

# Create data directories
mkdir -p data/raw

# Run the download script
python scripts/download_flight_data.py