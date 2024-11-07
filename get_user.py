import requests
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from cryptography.fernet import Fernet


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


def fetch_random_users(count=100):
    url = f'https://randomuser.me/api/?results={count}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data['results']
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return []

def anonymize_user_data(users):
    for user in users:
        # Anonymize 'first' and 'last' names
        user['name']['first'] = 'ANONYMIZED_FIRST'
        user['name']['last'] = 'ANONYMIZED_LAST'

        # Anonymize 'street' number and name
        user['location']['street']['number'] = 'ANONYMIZED_NUMBER'
        user['location']['street']['name'] = 'ANONYMIZED_STREET'

        # Anonymize 'coordinates'
        user['location']['coordinates']['latitude'] = '0.0000'
        user['location']['coordinates']['longitude'] = '0.0000'

        # Anonymize 'username' and 'password'
        user['login']['username'] = 'ANONYMIZED_USERNAME'
        user['login']['password'] = 'ANONYMIZED_PASSWORD'

        # Anonymize 'dob' (date of birth)
        user['dob']['date'] = '1970-01-01T00:00:00.000Z'
        user['dob']['age'] = 0

        # Anonymize 'phone' and 'cell'
        user['phone'] = 'ANONYMIZED_PHONE'
        user['cell'] = 'ANONYMIZED_CELL'
    return users

def flatten_user_data(users):
    # Flatten the nested JSON structures
    flat_users = []
    for user in users:
        flat_user = {}
        flat_user['gender'] = user.get('gender')

        # Flatten name
        name = user.get('name', {})
        flat_user['name_title'] = name.get('title')
        flat_user['name_first'] = name.get('first')
        flat_user['name_last'] = name.get('last')

        # Flatten location
        location = user.get('location', {})
        street = location.get('street', {})
        flat_user['street_number'] = street.get('number')
        flat_user['street_name'] = street.get('name')
        flat_user['city'] = location.get('city')
        flat_user['state'] = location.get('state')
        flat_user['country'] = location.get('country')
        flat_user['postcode'] = location.get('postcode')
        coordinates = location.get('coordinates', {})
        flat_user['latitude'] = coordinates.get('latitude')
        flat_user['longitude'] = coordinates.get('longitude')
        timezone = location.get('timezone', {})
        flat_user['timezone_offset'] = timezone.get('offset')
        flat_user['timezone_description'] = timezone.get('description')

        # Other fields
        flat_user['email'] = user.get('email')

        # Flatten login
        login = user.get('login', {})
        flat_user['login_uuid'] = login.get('uuid')
        flat_user['username'] = login.get('username')
        flat_user['password'] = login.get('password')
        flat_user['salt'] = login.get('salt')
        flat_user['md5'] = login.get('md5')
        flat_user['sha1'] = login.get('sha1')
        flat_user['sha256'] = login.get('sha256')

        # Flatten dob
        dob = user.get('dob', {})
        flat_user['dob_date'] = dob.get('date')
        flat_user['dob_age'] = dob.get('age')

        # Flatten registered
        registered = user.get('registered', {})
        flat_user['registered_date'] = registered.get('date')
        flat_user['registered_age'] = registered.get('age')

        flat_user['phone'] = user.get('phone')
        flat_user['cell'] = user.get('cell')

        # Flatten id
        id_info = user.get('id', {})
        flat_user['id_name'] = id_info.get('name')
        flat_user['id_value'] = id_info.get('value')

        # Flatten picture
        picture = user.get('picture', {})
        flat_user['picture_large'] = picture.get('large')
        flat_user['picture_medium'] = picture.get('medium')
        flat_user['picture_thumbnail'] = picture.get('thumbnail')

        flat_user['nat'] = user.get('nat')

        flat_users.append(flat_user)
    return flat_users

def encrypt_file(file_path, key):
    fernet = Fernet(key)
    with open(file_path, 'rb') as file:
        original = file.read()
    encrypted = fernet.encrypt(original)
    encrypted_file_path = file_path + '.enc'
    with open(encrypted_file_path, 'wb') as encrypted_file:
        encrypted_file.write(encrypted)
    return encrypted_file_path

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

    # Fetch and anonymize users
    users = fetch_random_users(100)
    if not users:
        print("No users fetched.")
        return

    anonymized_users = anonymize_user_data(users)

    # Flatten the data
    flat_users = flatten_user_data(anonymized_users)

    # Convert to DataFrame
    df = pd.DataFrame(flat_users)

    # Ensure 'postcode' is treated as a string
    df['postcode'] = df['postcode'].astype(str)

    # Save to Parquet
    parquet_file = 'users_anonymized.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

    # Generate encryption key
    encryption_key = Fernet.generate_key()

    # Encrypt the Parquet file
    encrypted_file = encrypt_file(parquet_file, encryption_key)

    # Ensure the bucket exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Upload the encrypted file to Minio
    try:
        minio_client.fput_object(bucket_name, object_name, encrypted_file)
        print(f"Encrypted Parquet file uploaded to Minio bucket '{bucket_name}' as '{object_name}'.")
    except Exception as e:
        print(f"An error occurred while uploading to Minio: {e}")
        return

    # Save the encryption key securely (this is critical)
    with open('encryption_key.key', 'wb') as key_file:
        key_file.write(encryption_key)
    print("Encryption key saved to 'encryption_key.key'. Keep this file secure!")

if __name__ == "__main__":
    main()
