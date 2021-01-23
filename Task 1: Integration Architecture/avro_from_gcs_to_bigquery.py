# from apache_beam.io import ReadFromAvro
# from apache_beam.io import WriteToBigQuery
# from apache_beam.pipeline import Pipeline
# from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io import BigQueryDisposition
import apache_beam as beam
import argparse
import json


PROJECT_ID = 'cedar-freedom-298301'
RUNNER = 'DataflowRunner'
TEMP_LOCATION = 'gs://big-data-training-julio/temp'
STAGING_LOCATION = 'gs://big-data-training-julio/staging'
REGION = 'us-west2'
JOB_NAME = 'avrotobigquery'
INPUT_FILE = 'gs://big-data-training-julio/test-dataset-2.avro-00000-of-00001'
DATASET_NAME = 'dataset_viajes'



def get_schema_from_gcs(bucket, file_name):
    gcs_client = beam.io.gcp.gcsio.GcsIO()
    with gcs_client.open(f"gs://{bucket}/{file_name}", mode='rb', mime_type="application/json") as f:
        content = f.readlines()
    return json.loads(content[0].decode('utf-8'))


if __name__ == "__main__":
    # Get schema
    # avro_schema = get_schema_from_gcs(bucket="big-data-training-julio", file_name="test-dataset_schema.json")

    # Create dataset in BigQuery, optional: create table

    # Pipeline args
    pipeline_args = [
        f'--project={PROJECT_ID}',
        f'--staging_location={STAGING_LOCATION}',
        f'--temp_location={TEMP_LOCATION}',
        f'--job_name={JOB_NAME}',
        f'--runner={RUNNER}',
        f'--region={REGION}'
    ]

    with beam.pipeline.Pipeline(argv=pipeline_args) as p:
        (
            p
            | 'Read Avro' >> beam.io.ReadFromAvro("gs://big-data-training-julio/test-dataset-2.avro-00000-of-00001")
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                f"{PROJECT_ID}:{DATASET_NAME}.avro_table",
                schema='SCHEMA_AUTODETECT',
                custom_gcs_temp_location=TEMP_LOCATION,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                )
        )
    
# python Task\ 1\:\ Integration\ Architecture/avro_from_gcs_to_bigquery.py
#  --runner DataFlowRunner
#  --project cedar-freedom-298301 
#  --temp_location gs://big-data-training-julio/temp 
#  --staging_location gs://big-data-training-julio/staging 
#  --region us-west2 
#  --job_name avrotobigquery