import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import parquetio
import logging
from datetime import datetime
from helper.functions import WriteToMinio, CustomOptions
import sys


def run(argv=None):
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    logger = logging.getLogger(__name__)

    options = PipelineOptions(argv)
    options = PipelineOptions(flags=argv)
    custom_options = options.view_as(CustomOptions)

    bucket_name = custom_options.minio_bucket
    endpoint = custom_options.minio_endpoint
    input_prefix = custom_options.minio_input_prefix
    output_prefix = custom_options.minio_output_prefix
    access_key = custom_options.minio_access_key
    secret_key = custom_options.minio_secret_key
    batch_size = custom_options.batch_size

    input_path = f's3://{bucket_name}/{input_prefix}/*.parquet'

    options = PipelineOptions([
        f'--s3_access_key_id={access_key}',
        f'--s3_secret_access_key={secret_key}',
        f'--s3_endpoint_url=http://{endpoint}',
    ])

    logger.info(f"Parameters: minio_endpoint={endpoint}, minio_bucket={bucket_name}, minio_prefix={input_prefix}, batch_size={batch_size}")

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadParquet' >> parquetio.ReadFromParquet(input_path)
            | 'FilterEurope' >> beam.Filter(lambda elem: elem.get('continent_exp', '') == 'Europe')
            | 'Batch elevemts' >> beam.BatchElements(min_batch_size=batch_size, max_batch_size=batch_size * 100)
            | 'WriteToMinio' >> beam.ParDo(WriteToMinio(endpoint, bucket_name, output_prefix, access_key, secret_key))
        )
    logger.info("Pipeline completed successfully.")

if __name__ == "__main__":
    run()