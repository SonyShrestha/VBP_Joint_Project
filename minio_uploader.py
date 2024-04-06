from minio import Minio
from minio.error import S3Error
import os
import configparser


def load_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def create_minio_client(endpoint, access_key, secret_key):
    # Create a client using the MinIO server endpoint, and access and secret keys.
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False  # Set to True if using HTTPS
    )

def upload_directory_to_bucket(minio_client, bucket_name, directory):
    try:
        # Check if the bucket already exists
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # Upload files in the directory
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                minio_client.fput_object(bucket_name, file_name, file_path)
                print(f"Uploaded '{file_name}' to bucket '{bucket_name}'.")
    except S3Error as exc:
        print("Error occurred:", exc)




def main(config_path):
    config = load_config(config_path)

    # Accessing configuration values

    endpoint = config['MINIO']['endpoint']
    access_key = config['MINIO']['access_key']
    secret_key = config['MINIO']['secret_key']
    bucket_name = config['MINIO']['bucket_name']
    directory = config['MINIO']['directory']


    minio_client = create_minio_client(endpoint, access_key, secret_key)
    upload_directory_to_bucket(minio_client, bucket_name, directory)

if __name__ == "__main__":
    main("./config.ini")