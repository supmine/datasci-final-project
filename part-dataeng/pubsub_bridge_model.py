import requests
from google.cloud import storage
from google.cloud import pubsub_v1 as pubsub


BUCKET_NAME = "datasci-kalampree"
img_list_local_path = "./img_list.txt"
img_list_storage_path = "radarNJ_latest.txt"

project_id = "datasci-kalampree"
pub_topic_id = "finished-inference-trigger"
subscription_id = "model-server"


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from the bucket."""
    
    # The name of your GCS bucket
    # bucket_name = "your-bucket-name"
    
    # The name of the file in GCS bucket to download
    # source_blob_name = "storage-object-name"
    
    # The path and the file once downloaded
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)


def read_txt_file(file_name):
    fp = open(file_name, 'r')
    lines = fp.readlines()
    fp.close()
    return lines


def model_predict(filepaths):
    r = requests.post('http://localhost:8000/predict/',
        json={
            "filepaths": filepaths
        }
    )
    return r.json()["prediction"]


publisher = pubsub.PublisherClient()
topic_path = publisher.topic_path(project_id, pub_topic_id)

subscriber = pubsub.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print("Initialized")
while True:
    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 5,
        }
    )
    if len(response.received_messages) > 0:
        for msg in response.received_messages:
            try:
                print("Received message", msg.message.data)
                download_blob(BUCKET_NAME, img_list_storage_path, img_list_local_path)
                lines = read_txt_file(img_list_local_path)
                filepaths = [fp.strip() for fp in lines]
                print(filepaths)
                pred_filepaths = model_predict(filepaths)
                print("Predictions:", pred_filepaths)
                payload = ",".join(pred_filepaths)
                pub_future = publisher.publish(topic_path, payload.encode("utf-8"))
                print("Published:", pub_future.result())
            except:
                print("Error for the message")
                continue
            
        ack_ids = [msg.ack_id for msg in response.received_messages]
        print("Acked:", ack_ids)
        subscriber.acknowledge(
            request={
                "subscription": subscription_path,
                "ack_ids": ack_ids,
            }
        )
