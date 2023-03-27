import requests
from datetime import datetime
import calendar
import os
from google.cloud import storage

IMG_URL = 'https://weather.bangkok.go.th/Images/Radar/NKKML/nkRadarOnGoogle.png'


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    
    # The name of your GCS bucket
    # bucket_name = "your-bucket-name"
    
    # The path and the file to upload
    # source_file_name = "local/path/to/file"
    
    # The name of the file in GCS bucket once uploaded
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)


def download_image(image_url):
    current_datetime = datetime.utcnow()
    utc_time = calendar.timegm(current_datetime.utctimetuple())
    img_data = requests.get(image_url).content
    filename = f'{utc_time}.png'
    filepath = '/tmp/' + filename
    with open(filepath, 'wb') as handler:
        handler.write(img_data)
    handler.close()
    return filename, filepath


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    # print(pubsub_message)

    BUCKET_NAME = os.environ.get('BUCKET_NAME', 'Specified environment variable is not set.')
    img_name, img_path = download_image(IMG_URL)
    upload_blob(BUCKET_NAME, img_path, "radar/" + img_name)