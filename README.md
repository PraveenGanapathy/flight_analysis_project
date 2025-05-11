# 🛩️ Flight Analysis Project

## 📊 Overview

This project provides a comprehensive flight analysis solution using **modern data engineering tools**:
- **Apache Airflow** for workflow orchestration
- **Streamlit** for interactive dashboards
- **Python** for data processing

## 🏗️ Architecture

![Project Architecture](./public/architecture.png)

## 🎬 Demo

![Project Demo](./public/demo.gif)

## 🚀 Setup Options

### 1. Docker-Powered Setup 🐳

#### Prerequisites
- **Docker** and **Docker Compose** installed
- **AWS credentials** for data storage and processing

#### Deployment Steps
1. **Configure AWS Credentials**
   - Create a `.env` file in the project root
   - Add your AWS credentials:
     ```env
     AWS_ACCESS_KEY_ID=your_access_key
     AWS_SECRET_ACCESS_KEY=your_secret_key
     REGION_NAME=your_region
     ```

2. **Launch Containers**
   ```bash
   docker-compose up -d
   ```

### 2. Manual Setup without Docker 💻

#### Prerequisites
- Python 3.7+ installed
- AWS credentials for data storage and processing

#### Deployment Steps
1. **Create Virtual Environment**
   ```bash
   python3 -m venv venv
   # Activate:
   # - Linux/MacOS: source venv/bin/activate
   # - Windows: venv\Scripts\activate
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment**
   - Create `.env` file with AWS credentials
   - Add necessary configurations

4. **Initialize Airflow**
   ```bash
   # Initialize database
   airflow db init

   # Create admin user
   airflow users create \
       --username admin \
       --password admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com

   # Start services
   # Terminal 1: airflow scheduler
   # Terminal 2: airflow webserver
   ```

5. **Launch Streamlit Dashboard**
   ```bash
   cd dashboard
   streamlit run flight_etl_dag.py
   ```

## 🌐 Access Points

| Service | URL |
|---------|-----|
| Airflow Web Interface | [http://localhost:8080](http://localhost:8080) |
| Streamlit Dashboard | [http://localhost:8504](http://localhost:8504) |

## 📂 Project Structure

```
flight-analysis/
│
├── airflow/          # Workflow definitions
│   └── dags/         # Airflow DAGs
│
├── dashboard/        # Interactive visualizations
│
├── data/             # Raw and processed data
│
├── notebooks/        # Exploratory data analysis
│
├── scripts/          # Utility and helper scripts
│
└── config/           # Configuration management
```

## 🚦 Getting Started

1. Access **Airflow web interface** to:
   - Trigger data processing workflows
   - Monitor job status and logs

2. Explore **Streamlit dashboard** to:
   - Visualize flight data insights
   - Interact with dynamic charts and reports
