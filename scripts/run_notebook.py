import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import logging

logger = logging.getLogger(__name__)

def run_notebook(**context):
    """Execute the analysis notebook"""
    try:
        # Read the notebook
        with open('/opt/airflow/notebooks/flight_analysis.ipynb') as f:
            nb = nbformat.read(f, as_version=4)
        
        # Execute the notebook
        ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
        ep.preprocess(nb)
        
        # Save executed notebook
        with open('/opt/airflow/notebooks/executed_flight_analysis.ipynb', 'w') as f:
            nbformat.write(nb, f)
            
        logger.info("Notebook executed successfully")
        return "Notebook execution completed"
        
    except Exception as e:
        logger.error(f"Error executing notebook: {str(e)}")
        raise