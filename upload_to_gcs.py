# upload to gcs
# Usage: python upload_to_gcs.py <bucket_name> <file_path> <destination_path>

import os
import sys

from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud import bigquery

GCP_SERVICE_ACCOUNT_JSON = "secret/etl-training-427511-8a5bde222176.json"

if __name__ == '__main__':
    bucket_name = "muic-etl"
    file_path = "generated/exam_grades-from-db-tf.csv"
    destination_path = "data/exam_grades-from-db-tf.csv"

    storage_client = storage.Client.from_service_account_json(GCP_SERVICE_ACCOUNT_JSON)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(file_path)
    print(f"File {file_path} uploaded to {destination_path} in bucket {bucket_name}.")


    # load csv to bigquery
    client = bigquery.Client.from_service_account_json(GCP_SERVICE_ACCOUNT_JSON)
    dataset_id = "examx"
    table_id = "exam_data"
    source_uri = f"gs://{bucket_name}/{destination_path}"
    # check if dataset exists
    try:
        dataset_ref = client.get_dataset(dataset_id)
    except NotFound as e:
        # requires bigquery.datasets.create permission
        dataset_ref = client.create_dataset(dataset_id)

    # truncate table before loading
    # table_ref = dataset_ref.table(table_id)
    # table = client.get_table(table_ref)
    # print(f"Table {table_id} contains {table.num_rows} rows.")
    # print(f"Truncating table {table_id}...")
    # client.delete_table(table_ref)



    # run query to delete table
    # query = f"DELETE FROM {dataset_id}.{table_id} WHERE true"
    # query_job = client.query(query)
    # query_job.result()
    # print(f"Deleted {query_job.num_dml_affected_rows} rows.")


    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    load_job = client.load_table_from_uri(
        source_uri, dataset_ref.table(table_id), job_config=job_config
    )
    print(f"Starting job {load_job.job_id}")
    load_job.result()
    print(f"Job finished")
    destination_table = client.get_table(dataset_ref.table(table_id))
    print(f"Loaded {destination_table.num_rows} rows to {dataset_id}.{table_id}")



