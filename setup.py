import os
from google.cloud import storage, bigquery
from dotenv import load_dotenv

credential_path = "./key.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

load_dotenv()
project_id = os.getenv('PROJECT_ID')
dataset_id = "google_trends"
table_id = [
    "international_top_rising_terms",
    "international_top_terms",
    "top_rising_terms",
    "top_terms"
]
source_bucket = f"{project_id}-source"


def create_source_bucket(project_id: str):
    """
    Create source bucket to be used in the project

    Parameters:
    PROJECT_ID (str): Google cloud project id.
    storage_client: Initialized client object from Google cloud storage module.
    """

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()
    bucket_name = f"{project_id}-source"

    if bucket_name in buckets:
        print(f"Bucket {bucket_name} already exists")
    else:
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket.name} created")

    assert storage_client.get_bucket(bucket_name).exists()


def export_bigquery_table_to_source_bucket(project_id: str,
                                           dataset_id: str,
                                           table_id: list,
                                           source_bucket: str):
    """
    Export bigquery table to source bucket

    Parameters:
    project_id (str): Google cloud project id.
    dataset_id (str): Initialized client object from Google cloud bigquery module.
    table_id   (str):
    source_bucket (str):
    """

    bigquery_client = bigquery.Client()

    for table in table_id:
        destination_uri = f"gs://{source_bucket}/{table}.csv"
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        table_ref = dataset_ref.table(table)

        extract_job = bigquery_client.extract_table(
            table_ref,
            destination_uri,
            location="US",
        )
        extract_job.result()

        print(f"Exported {table} to {source_bucket}.")


if __name__ == "__main__":
    create_source_bucket(project_id)
