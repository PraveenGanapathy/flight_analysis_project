# Flight Analysis Project

## Setup
1. Update .env with AWS credentials
2. Run `docker-compose up -d`

## Access
- Airflow: http://localhost:8080
- Dashboard: http://localhost:8501

## Structure
├── airflow/dags/    # Airflow DAGs
├── dashboard/       # Streamlit dashboard
├── data/raw/       # Data storage
├── scripts/        # Utility scripts
└── config/         # Configurations
