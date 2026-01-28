import functions_framework
from google.cloud import storage
import json

# Helper function to rename keys in the JSON object
def rename_vendor_keys(obj):
    """ Recursively rename keys in the JSON object.
    Arguments:
    obj -- The JSON object to be processed.
    """
    if isinstance(obj, dict):
        return {
            key.replace("vendor: ", "vendor"): rename_vendor_keys(value)
            for key, value in obj.items()
        }
    elif isinstance(obj, list):
        return [rename_vendor_keys(item) for item in obj]
    return obj

@functions_framework.cloud_event
def rename_vendor_and_upload(cloud_event):
    """Triggered by a change in a storage bucket.
    Args:
        cloud_event (CloudEvent): The CloudEvent object.
    """
    event = cloud_event.data

    bucket_name = event["bucket"]
    file_name = event["name"]
    
    # change from json to .jsonl 
    output_file_name = f"processed_{file_name.replace('.json', '.jsonl')}" 

    storage_client = storage.Client()

    print(f"Input file name: {file_name}")
    print(f"Output file name: {output_file_name}")
    
    input_bucket = storage_client.bucket(bucket_name)
    input_blob = input_bucket.blob(file_name)

    output_bucket_name = "d607pa2-export"
    output_bucket = storage_client.bucket(output_bucket_name)
    output_blob = output_bucket.blob(output_file_name)

    try:
        json_data = json.loads(input_blob.download_as_text())

        cleaned_data = [rename_vendor_keys(item) for item in json_data]
        ndjson_output = "\n".join(json.dumps(item) for item in cleaned_data)

        output_blob.upload_from_string(ndjson_output, content_type="application/jsonl")
        print(f"Uploaded cleaned file to: gs://{output_bucket_name}/{output_file_name}")

    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        raise e
