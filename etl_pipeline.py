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

# Standardize phone numbers to the format +1-XXX-XXX-XXXX
def standardize_phone_number(phone):
    try:
        phone = re.sub(r'\D', '', phone)  # Remove non-digit characters
        if len(phone) == 10:  # Assume US numbers without country code
            phone = f"+1-{phone[:3]}-{phone[3:6]}-{phone[6:]}"
        return phone
    except Exception as e:
        logging.error(f"Error standardizing phone number {phone}: {e}")
        return None

# Standardize addresses: lowercase, remove extra spaces
def standardize_address(address):
    try:
        return ' '.join(address.strip().lower().split())
    except Exception as e:
        logging.error(f"Error standardizing address {address}: {e}")
        return None

# De-identification function for sensitive fields
def hash_sensitive_info(value):
    try:
        if isinstance(value, str):
            return hashlib.sha256(value.encode()).hexdigest()
        return value
    except Exception as e:
        logging.error(f"Error hashing sensitive info {value}: {e}")
        return None

# Setup connection to LocalStack S3
def setup_localstack_s3():
    try:
        s3 = boto3.resource('s3', endpoint_url='http://localstack:4566')
        bucket_name = 'health-data'
        if not any(bucket.name == bucket_name for bucket in s3.buckets.all()):
            s3.create_bucket(Bucket=bucket_name)
        logging.info(f"LocalStack S3 bucket '{bucket_name}' is set up.")
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

        # Standardize phone numbers and addresses
        logging.info("Standardizing phone numbers and addresses.")
        patient_data['phone_number'] = patient_data['phone_number'].apply(standardize_phone_number)
        patient_data['address'] = patient_data['address'].apply(standardize_address)

        # De-identify sensitive patient data
        logging.info("De-identifying patient data.")
        patient_data['name'] = patient_data['name'].apply(hash_sensitive_info)
        patient_data['address'] = patient_data['address'].apply(hash_sensitive_info)
        patient_data['phone_number'] = patient_data['phone_number'].apply(hash_sensitive_info)

        # Join the patient and appointment data on patient_id
        logging.info("Joining patient and appointment data.")
        joined_data = pd.merge(patient_data, appointment_data, on='patient_id')

        # Save joined data as Parquet
        output_parquet_path = 'joined_data.parquet'
        table = pa.Table.from_pandas(joined_data)
        pq.write_table(table, output_parquet_path)

        logging.info(f"Data processing complete. Output saved as {output_parquet_path}.")
        return output_parquet_path
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

# Upload Parquet file to LocalStack S3
def upload_to_localstack(s3, parquet_file):
    try:
        logging.info(f"Uploading {parquet_file} to LocalStack S3.")
        s3.Bucket('health-data').upload_file(parquet_file, parquet_file)
        logging.info(f"Uploaded {parquet_file} to LocalStack S3.")
    except Exception as e:
        logging.error(f"Error uploading to LocalStack S3: {e}")
        raise

# PySpark Data Join and Display
def load_and_join_pyspark():
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
    
        # Read the Parquet file from LocalStack S3 using PySpark
        logging.info("Reading Parquet file from LocalStack S3 using PySpark.")
        patient_df = spark.read.parquet('s3a://health-data/joined_data.parquet')
    
        # Show the resulting dataframe
        logging.info("Displaying the joined data using PySpark.")
        patient_df.show()
    except Exception as e:
        logging.error(f"Error with PySpark operations: {e}")
        raise


# Main ETL Pipeline Execution
def main(patient_file, appointment_file):
    try:
        # Process and save data
        parquet_file = process_data(patient_file, appointment_file)

        # Setup LocalStack and upload to S3
        s3 = setup_localstack_s3()
        upload_to_localstack(s3, parquet_file)

        # Load and join data using PySpark
        load_and_join_pyspark()
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    # Define file paths for patient and appointment data
    patient_file = 'patient_data.csv'
    appointment_file = 'appointment_data.csv'

    # Run the ETL pipeline
    main(patient_file, appointment_file)
