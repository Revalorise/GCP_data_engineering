import os
from google.cloud import storage, bigquery
from google.api_core.exceptions import Conflict
from dotenv import load_dotenv

credential_path = "./gcp-data-engineering-key.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

load_dotenv()
project_id = os.getenv('PROJECT_ID')


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


def export_bigquery_result_to_source_bucket(country_names: list,
                                            country_codes: list,
                                            table_names: list,
                                            source_bucket: str):
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

    for table in table_names:
        for i, country in enumerate(country_names):
            query = f"""
                SELECT * 
                FROM `bigquery-public-data.census_bureau_international.{table}` 
                WHERE country_name = "{country}"
                ORDER BY year 
                DESC LIMIT 100;
            """

            file_name = f"{country_codes[i]}_{table}.csv"
            print(f"Exporting {country} data from {table} table...")

            try:
                query_job = bigquery_client.query(query)
                result_df = query_job.result().to_dataframe()

                destination_blob = storage_client.bucket(source_bucket).blob(file_name)
                destination_blob.content_type = "text/csv"
                with destination_blob.open('w') as blob:
                    result_df.to_csv(blob, index=False)

                print(f'The query results are exported to {source_bucket}\n')
            except Exception as e:
                print(f"An error occurred while processing {country} data from {table} table: {e}")

    print("Export completed")


if __name__ == "__main__":

    country_names = ["Thailand", "Japan", "United States"]
    country_codes = ["TH", "JA", "US"]
    table_names = [
        "mortality_life_expectancy",
        "birth_death_growth_rates",
        "midyear_population_agespecific"
    ]
    source_bucket = f"{project_id}-source"

    export_bigquery_result_to_source_bucket(country_names,
                                            country_codes,
                                            table_names,
                                            source_bucket)
