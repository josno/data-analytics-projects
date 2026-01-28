import functions_framework
from google.cloud import bigquery

# Adds ndjson data to BigQuery
@functions_framework.cloud_event
def load_to_bigquery(cloud_event):
    """Triggered by a change in a storage bucket.
    Args:
        cloud_event (CloudEvent): The CloudEvent object.
    """
    #initialize BigQuery client
    project_id = "d607pa2-456203"  # Replace with your actual project ID
    bigquery_client = bigquery.Client(project=project_id)

    # Initialize cloud event
    event = cloud_event.data

    bucket_name = event['bucket']
    file_name = event['name']
    source_uri = f"gs://{bucket_name}/{file_name}"

    dataset_name = "ecommerce"
    table_name = "staging"
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,  
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    try:
        load_job = bigquery_client.load_table_from_uri(
            source_uri, table_id, location="us-central1", job_config=job_config
        )

        load_job.result()  # Wait to complete

        table = bigquery_client.get_table(table_id)
        print(f"Loaded {table_id} from {file_name} into {table}")

    except Exception as e:
        print(f"Error loading {file_name} to BigQuery: {e}")
        raise e
