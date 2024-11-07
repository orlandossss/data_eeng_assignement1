import os
import json
import pandas as pd
import pyarrow.parquet as pq
from minio import Minio
from cryptography.fernet import Fernet
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def load_config(config_file='config.json'):
    """
    Loads the configuration from a JSON file.
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        print("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        print(f"Configuration file '{config_file}' not found.")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing configuration file: {e}")
        exit(1)

def download_encrypted_file(minio_client, bucket_name, object_name, local_file_path):
    """
    Downloads the encrypted file from Minio to the local filesystem.
    """
    try:
        minio_client.fget_object(bucket_name, object_name, local_file_path)
        print(f"Downloaded '{object_name}' from bucket '{bucket_name}' to '{local_file_path}'.")
    except Exception as e:
        print(f"An error occurred while downloading from Minio: {e}")
        exit(1)

def decrypt_file(encrypted_file_path, decrypted_file_path, key):
    """
    Decrypts the encrypted file using the provided key.
    """
    fernet = Fernet(key)
    with open(encrypted_file_path, 'rb') as enc_file:
        encrypted = enc_file.read()
    decrypted = fernet.decrypt(encrypted)
    with open(decrypted_file_path, 'wb') as dec_file:
        dec_file.write(decrypted)
    print(f"Decrypted file saved as '{decrypted_file_path}'.")

def read_parquet_file(parquet_file_path):
    """
    Reads the Parquet file into a pandas DataFrame.
    """
    table = pq.read_table(parquet_file_path)
    df = table.to_pandas()
    print(f"Data loaded into DataFrame with {len(df)} records.")
    return df

def analyze_data(df):
    """
    Performs the required data analysis and outputs the results.
    """
    # Question 1: What is the average duration since a user registered?
    df['registered_date'] = pd.to_datetime(df['registered_date']).dt.tz_localize(None)
    now = datetime.now()
    df['days_since_registered'] = (now - df['registered_date']).dt.days
    average_duration = df['days_since_registered'].mean()
    print(f"\nAverage duration since users registered: {average_duration:.2f} days")

    # Question 2: What is the age and gender distribution of users?
    # Print the number of male and female users
    gender_counts = df['gender'].value_counts()
    print("\nNumber of users by gender:")
    print(gender_counts)

    # Question 3: Which location (country and city) do most users come from?
    location_counts = df.groupby(['country', 'city']).size().reset_index(name='counts')
    top_locations = location_counts.sort_values(by='counts', ascending=False).head(10)
    print("\nTop 10 Locations (Country and City) by Number of Users:")
    print(top_locations)

def main():
    
    config = load_config()
    # Minio Configuration
    minio_config = config['minio']
    minio_client = Minio(
        endpoint=minio_config['endpoint'],
        access_key=minio_config['access_key'],
        secret_key=minio_config['secret_key'],
        secure=minio_config['secure']
    )

    bucket_name = minio_config['bucket_name']
    object_name = minio_config['object_name']

    encrypted_file_path = 'downloaded_' + object_name
    decrypted_file_path = 'decrypted_' + object_name.replace('.enc', '')
    # Download the encrypted file from Minio
    download_encrypted_file(minio_client, bucket_name, object_name, encrypted_file_path)

    # Read the encryption key
    try:
        with open('encryption_key.key', 'rb') as key_file:
            encryption_key = key_file.read()
        print("Encryption key loaded successfully.")
    except FileNotFoundError:
        print("Encryption key file not found. Ensure 'encryption_key.key' is present.")
        exit(1)

    # Decrypt the file
    decrypt_file(encrypted_file_path, decrypted_file_path, encryption_key)

    # Read the decrypted Parquet file into a DataFrame
    df = read_parquet_file(decrypted_file_path)

    # Analyze the data
    analyze_data(df)

    # Clean up local files (optional)
    os.remove(encrypted_file_path)
    os.remove(decrypted_file_path)
    print("Temporary files removed.")

if __name__ == "__main__":
    main()
