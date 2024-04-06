import docker
import time
from minio import Minio
from minio.error import S3Error
import configparser
import os

# Get the path to the parent parent directory
config_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir, os.pardir))
print(config_dir)

# Specify the path to config file
config_file_path = os.path.join(config_dir, "config.ini")

# reading the config file
config = configparser.ConfigParser()
config.read(config_file_path)
raw_data_folder_path= config['COMMON']['raw_data_dir']

def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio("localhost:9000",
    access_key="spicibytes",
    secret_key="spicybytes123",
    secure=False
    )

    # The file to upload, change this path if needed
    # source_file = "D:/BDMA/UPC/BDM/P1/VBP_Joint_Project/data/raw/ApprovedFood.json"

    # The destination bucket and filename on the MinIO server
    bucket_name = "spicybytes-bucket"
    destination_file = "ApprovedFood.json"
    
    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # Upload the file, renaming it in the process
    for filename in os.listdir(raw_data_folder_path):
        filepath= os.path.join(raw_data_folder_path, filename)
        destination_file= filename
        client.fput_object(
            bucket_name, destination_file, filepath,
        )
        print(
            filepath, "successfully uploaded as object",
            destination_file, "to bucket", bucket_name,
        )

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)