"""
Databricks ingest task for patchit-ingest-job.
Handles data ingestion with schema drift detection and remediation.
"""
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import TimestampType
from datetime import datetime


def validate_and_fix_schema(df: DataFrame, required_columns: list) -> DataFrame:
    """
    Validate schema and add missing required columns with default values.
    
    Args:
        df: Input DataFrame
        required_columns: List of required column names
        
    Returns:
        DataFrame with all required columns present
    """
    existing_columns = df.columns
    missing_columns = [col for col in required_columns if col not in existing_columns]
    
    if missing_columns:
        print(f"Schema drift detected: missing columns {missing_columns}")
        
        # Add missing columns with appropriate default values
        for col_name in missing_columns:
            if col_name == 'event_ts':
                # Add event_ts with current timestamp if missing
                df = df.withColumn(col_name, current_timestamp())
                print(f"Fixed: Added missing column '{col_name}' with current_timestamp()")
            else:
                # Add other missing columns with null values
                df = df.withColumn(col_name, lit(None))
                print(f"Fixed: Added missing column '{col_name}' with null value")
    
    return df


def ingest_task():
    """
    Main ingest task for Databricks job.
    Reads data, validates schema, and writes output.
    """
    spark = SparkSession.builder.appName("patchit-ingest-job").getOrCreate()
    
    # Required columns for the schema
    required_columns = ['event_ts']
    
    try:
        # Read input data (adjust path as needed)
        # This is a placeholder - actual source path should be configured
        input_path = "/mnt/landing/input_data"
        
        print(f"Reading data from: {input_path}")
        df = spark.read.format("parquet").load(input_path)
        
        print(f"Input schema: {df.schema}")
        print(f"Input columns: {df.columns}")
        
        # Validate and fix schema drift
        df_fixed = validate_and_fix_schema(df, required_columns)
        
        print(f"Fixed schema: {df_fixed.schema}")
        print(f"Fixed columns: {df_fixed.columns}")
        
        # Write output (adjust path as needed)
        output_path = "/mnt/processed/output_data"
        print(f"Writing data to: {output_path}")
        
        df_fixed.write.format("delta").mode("append").save(output_path)
        
        print("Ingest task completed successfully")
        
    except Exception as e:
        print(f"Error in ingest_task: {str(e)}")
        raise


if __name__ == "__main__":
    ingest_task()
