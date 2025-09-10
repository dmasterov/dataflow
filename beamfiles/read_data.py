import apache_beam as beam
from apache_beam.io import parquetio
from apache_beam.options.pipeline_options import PipelineOptions
import os

def run():
    # Use s3:// prefix with bucket and path inside MinIO
    input_path = 's3://input/data/*.parquet'

    # Beam pipeline options
    options = PipelineOptions([
        '--s3_access_key_id=minioadmin',
        '--s3_secret_access_key=minioadmin',
        '--s3_endpoint_url=http://minio:9000',
    ])

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadParquet' >> parquetio.ReadFromParquet(input_path)
            | 'Print' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()
