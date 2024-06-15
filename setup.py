import os
from google.cloud import storage, bigquery
from google.api_core.exceptions import Conflict
from dotenv import load_dotenv

credential_path = "./gcp-data-engineering-key.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

load_dotenv()
project_id = os.getenv('PROJECT_ID')

country_name = ["Thailand", "Japan", "United States"]
country_code = ["TH", "JA", "US"]

source_bucket = f"{project_id}-source"


def create_source_bucket(project_id: str):
    """
    Create source bucket to be used in the project

    Parameters:
    PROJECT_ID (str): Google cloud project id.
    storage_client: Initialized client object from Google cloud storage module.
    """

    storage_client = storage.Client()
    bucket_name = f"{project_id}-source"

    try:
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket.name} created")
    except Conflict:
        print(f"Bucket {bucket_name} already exists")

    assert storage_client.get_bucket(bucket_name).exists()


def export_bigquery_result_to_source_bucket(country_name: list,
                                            country_code: list):
    """
    Export bigquery queries to source bucket in csv format.

    This function only supports in exporting Census bureau international mortality
    and life expectancy daya

    Parameters:
    country_name (list): List of country name to be exported.
    country_code (list): List of country code e.g. ("TH", "JP").
    """

    bigquery_client = bigquery.Client()
    storage_client = storage.Client()

    for i in range(len(country_name)):
        query = f"""
            SELECT * 
            FROM `bigquery-public-data.census_bureau_international.mortality_life_expectancy` 
            WHERE country_name = "{country_name[i]}"
            ORDER BY year 
            DESC LIMIT 100;
        """

        file_name = f"{country_code[i]}_mortality_life_expectancy.csv"

        query_job = bigquery_client.query(query)
        destination_blob = storage_client.bucket(source_bucket).blob(file_name)
        destination_blob.content_type = "text/csv"
        query_job.result().to_dataframe().to_csv(destination_blob.open('w'), index=False)

        bucket = storage_client.get_bucket(source_bucket)
        blob = bucket.get_blob(file_name)
        print(f'The query results are exported to {blob.public_url}')

    print("Export completed")


if __name__ == "__main__":
    create_source_bucket(project_id)
    export_bigquery_result_to_source_bucket(country_name, country_code)
