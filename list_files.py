from google.cloud import storage

def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client.from_service_account_json('C:\\UPC\\BDM\\Project\\VBP_Joint_Project\\gcs_config.json')
    bucket = storage_client.bucket(bucket_name)
    print(bucket_name)

    blobs = bucket.list_blobs()

    for blob in blobs:
        print(blob.name)

if __name__ == '__main__':
    print("hello")
    list_blobs('spicy_1')
