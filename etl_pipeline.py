import os
import hashlib
import pandas as pd
import re
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the S3 bucket name from environment variable or use a default value
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Standardize phone numbers to the format +1-XXX-XXX-XXXX
def phone_number_standardization(phone):
    try:
        phone = re.sub(r'\D', '', phone)  # Remove non-digit characters
        if len(phone) == 10:  # Assume US numbers without country code
            phone = f"+1-{phone[:3]}-{phone[3:6]}-{phone[6:]}"
        return phone
    except Exception as e:
        logging.error(f"Error standardizing phone number {phone}: {e}")
        raise e

# Standardize addresses: lowercase, remove extra spaces
def address_standardization(address):
    try:
        return ' '.join(address.strip().lower().split())
    except Exception as e:
        logging.error(f"Error standardizing address {address}: {e}")
        raise e

# De-identification function for sensitive fields
def hash_sensitive_information(value):
    try:
        if isinstance(value, str):
            return hashlib.sha256(value.encode()).hexdigest()
        return value
    except Exception as e:
        logging.error(f"Error hashing sensitive info {value}: {e}")
        raise e

# Setup connection to LocalStack S3
def setup_localstack_s3():
    try:
        s3 = boto3.resource('s3', endpoint_url='http://localstack:4566')
        if not any(bucket.name == BUCKET_NAME for bucket in s3.buckets.all()):
            s3.create_bucket(Bucket=BUCKET_NAME)
        logging.info(f"LocalStack S3 bucket '{BUCKET_NAME}' is set up.")
        return s3
    except Exception as e:
        logging.error(f"Error setting up LocalStack S3: {e}")
        raise

# Data Ingestion and Processing
def process_data(patient_file, appointment_file):
    try:
        # Read the CSV files
        logging.info("Reading patient and appointment CSV files.")
        patient_data = pd.read_csv(patient_file)
        appointment_data = pd.read_csv(appointment_file)

        # Check for duplicates in patient_id
        if patient_data['patient_id'].duplicated().any():
            logging.warning("Duplicate patient_id found in patient data.")
            patient_data = patient_data.drop_duplicates(subset='patient_id')
        
        if appointment_data['patient_id'].duplicated().any():
            logging.warning("Duplicate patient_id found in appointment data.")
            appointment_data = appointment_data.drop_duplicates(subset='patient_id')

        # Standardize phone numbers and addresses
        logging.info("Standardizing phone numbers and addresses.")
        patient_data['phone_number'] = patient_data['phone_number'].apply(phone_number_standardization)
        patient_data['address'] = patient_data['address'].apply(address_standardization)

        # De-identify sensitive patient data
        logging.info("De-identifying patient data.")
        patient_data['name'] = patient_data['name'].apply(hash_sensitive_information)
        patient_data['address'] = patient_data['address'].apply(hash_sensitive_information)
        patient_data['phone_number'] = patient_data['phone_number'].apply(hash_sensitive_information)

        # Remove any columns that might have been added
        patient_data = patient_data.loc[:, ~patient_data.columns.str.contains('^_.*_column_0$')]
        appointment_data = appointment_data.loc[:, ~appointment_data.columns.str.contains('^_.*_column_0$')]

        # Save patient and appointment data as Parquet
        patient_parquet_path = 'patient_data.parquet'
        appointment_parquet_path = 'appointment_data.parquet'
        
        patient_table = pa.Table.from_pandas(patient_data, preserve_index=False)
        pq.write_table(patient_table, patient_parquet_path)

        appointment_table = pa.Table.from_pandas(appointment_data, preserve_index=False)
        pq.write_table(appointment_table, appointment_parquet_path)

        logging.info(f"Data processing complete. Patient data saved as {patient_parquet_path} and appointment data saved as {appointment_parquet_path}.")
        return patient_parquet_path, appointment_parquet_path
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

# Upload Parquet files to LocalStack S3
def upload_to_localstack(s3, parquet_files):
    try:
        for parquet_file in parquet_files:
            logging.info(f"Uploading {parquet_file} to LocalStack S3.")
            s3.Bucket(BUCKET_NAME).upload_file(parquet_file, parquet_file)
            logging.info(f"Uploaded {parquet_file} to LocalStack S3.")
    except Exception as e:
        logging.error(f"Error uploading to LocalStack S3: {e}")
        raise

# PySpark Data Join and Display
def join_using_pyspark():
    try:
        logging.info("Initializing PySpark session.")
        
        # Initialize Spark session with necessary packages for S3 support
        spark = SparkSession.builder \
            .appName('ETL Pipeline') \
            .config('spark.hadoop.fs.s3a.endpoint', 'http://localstack:4566') \
            .config('spark.hadoop.fs.s3a.access.key', 'test') \
            .config('spark.hadoop.fs.s3a.secret.key', 'test') \
            .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.888') \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
            .getOrCreate()
    
        # Read the Parquet files from LocalStack S3 using PySpark
        logging.info("Reading patient and appointment Parquet files from LocalStack S3 using PySpark.")
        patient_df = spark.read.parquet(f's3a://{BUCKET_NAME}/patient_data.parquet')
        appointment_df = spark.read.parquet(f's3a://{BUCKET_NAME}/appointment_data.parquet')       

        # Drop any columns that might have been added during the save/load process
        columns_to_drop = [col for col in patient_df.columns if col.startswith('_') and col.endswith('_column_0')]
        patient_df = patient_df.drop(*columns_to_drop)
        
        columns_to_drop = [col for col in appointment_df.columns if col.startswith('_') and col.endswith('_column_0')]
        appointment_df = appointment_df.drop(*columns_to_drop)

        # Join the two DataFrames on patient_id
        logging.info("Joining patient and appointment data.")
        joined_df = patient_df.join(appointment_df, on='patient_id', how='inner')

        # Remove any automatically added index columns
        columns_to_drop = [col for col in joined_df.columns if col.startswith('_') and col.endswith('_column_0')]
        joined_df = joined_df.drop(*columns_to_drop)

        # Show the resulting DataFrame
        logging.info("Displaying the joined data using PySpark.")
        joined_df.show()
        
      
    except Exception as e:
        logging.error(f"Error with PySpark operations: {e}")
        raise

# Main ETL Pipeline Execution
def main(patient_file, appointment_file):
    try:
        # Process and save data
        parquet_files = process_data(patient_file, appointment_file)

        # Setup LocalStack and upload to S3
        s3 = setup_localstack_s3()
        upload_to_localstack(s3, parquet_files)

        # Load and join data using PySpark
        join_using_pyspark()
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    # Define file paths for patient and appointment data
    patient_file = 'patient_data.csv'
    appointment_file = 'appointment_data.csv'

    # Run the ETL pipeline
    main(patient_file, appointment_file)
