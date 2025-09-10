import logging
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from helper.functions import WriteToMinio, DownloadExternalParquetFile, CustomOptions


def run(argv=None):

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    logger = logging.getLogger(__name__)

    options = PipelineOptions(argv)
    options = PipelineOptions(flags=argv)
    custom_options = options.view_as(CustomOptions)

    parquet_url = custom_options.parquet_url
    endpoint = custom_options.minio_endpoint
    bucket_name = custom_options.minio_bucket
    output_prefix = custom_options.minio_output_prefix
    access_key = custom_options.minio_access_key
    secret_key = custom_options.minio_secret_key
    batch_size = custom_options.batch_size

    logger.info(f"Parameters: parquet_url={parquet_url}, minio_endpoint={endpoint}, minio_bucket={bucket_name}, minio_prefix={output_prefix}, batch_size={batch_size}")

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Start" >> beam.Create([None])
            | "Download Parquet" >> beam.ParDo(DownloadExternalParquetFile(parquet_url))
            | 'Batch elevemts' >> beam.BatchElements(min_batch_size=batch_size, max_batch_size=batch_size * 100)
            | "Write to MinIO" >> beam.ParDo(WriteToMinio(endpoint, bucket_name, output_prefix, access_key, secret_key))
        )
    logger.info("Pipeline completed successfully.")

if __name__ == "__main__":
    run()