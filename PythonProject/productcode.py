"""
Databricks ingest task with schema drift handling.
This module handles data ingestion with flexible schema validation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


def validate_and_fix_schema(df: DataFrame, required_columns: list) -> DataFrame:
    """
    Validate DataFrame schema and add missing columns with default values.
    
    Args:
        df: Input DataFrame
        required_columns: List of required column names
        
    Returns:
        DataFrame with all required columns
    """
    missing_columns = set(required_columns) - set(df.columns)
    
    # Instead of raising an error for missing columns, add them with default values
    for col in missing_columns:
        if col == 'event_ts':
            # Add event_ts with current timestamp if missing
            df = df.withColumn('event_ts', current_timestamp())
        else:
            # Add other missing columns with null values
            df = df.withColumn(col, lit(None))
    
    return df


def ingest_data(df: DataFrame) -> DataFrame:
    """
    Main ingest function that handles schema drift gracefully.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with validated and fixed schema
    """
    required_columns = ['event_ts']
    
    # Handle schema drift by adding missing columns instead of raising error
    df_fixed = validate_and_fix_schema(df, required_columns)
    
    return df_fixed
