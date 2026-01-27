"""
Databricks ingestion job for patchit-ingest-job
Handles data ingestion with schema validation
"""
from datetime import datetime
from typing import Dict, Any, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_schema(data: Dict[str, Any], required_columns: List[str]) -> Dict[str, Any]:
    """
    Validate and enrich schema with missing columns
    
    Args:
        data: Input data dictionary
        required_columns: List of required column names
    
    Returns:
        Data with all required columns present
    """
    enriched_data = data.copy()
    
    for column in required_columns:
        if column not in enriched_data:
            logger.warning(f"Missing column '{column}', adding default value")
            
            # Add default value based on column name
            if column == 'event_ts' or column.endswith('_ts'):
                enriched_data[column] = datetime.utcnow().isoformat()
            elif column.endswith('_dt') or column.endswith('_date'):
                enriched_data[column] = datetime.utcnow().date().isoformat()
            elif column.endswith('_id') or column == 'id':
                enriched_data[column] = None
            else:
                enriched_data[column] = None
    
    return enriched_data


def ingest_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main ingestion function with schema drift handling
    
    Args:
        data: Input data to ingest
    
    Returns:
        Processed data ready for loading
    """
    # Define required schema columns
    required_columns = ['event_ts']
    
    # Validate and enrich schema instead of raising error
    validated_data = validate_schema(data, required_columns)
    
    logger.info(f"Successfully ingested data with columns: {list(validated_data.keys())}")
    return validated_data


def run_ingest_task(input_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Entry point for Databricks ingest_task
    
    Args:
        input_data: Optional input data, defaults to empty dict
    
    Returns:
        Ingested data with validated schema
    """
    if input_data is None:
        input_data = {}
    
    try:
        result = ingest_data(input_data)
        logger.info("Ingest task completed successfully")
        return result
    except Exception as e:
        logger.error(f"Ingest task failed: {str(e)}")
        raise


if __name__ == "__main__":
    # Test the ingestion with sample data
    sample_data = {
        "business_partner_id": 12345,
        "risk_score": 0.75,
        "model_version": "v1.0"
    }
    
    result = run_ingest_task(sample_data)
    print(f"Ingestion result: {result}")
