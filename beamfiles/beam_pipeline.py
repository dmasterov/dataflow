# start of my apache beam pipeline
# this app should read in batch parquet data from minio container transform
# such as aggregation and filtering
# with apache beam and write the output to the same container
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import parquetio
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from io import BytesIO
from apache_beam.transforms import DoFn
import logging
import uuid
from datetime import datetime
import os

class FilterAndTransform(DoFn):
    def process(self, element):
        if element.get('continent_exp', '') ==  'Europe':
            element['processed_at'] = datetime.utcnow().isoformat()
            yield element
        else:
            pass

class WriteToMinio(DoFn):
    def __init__(self, endpoint: str, bucket_name: str, object_prefix: str):
        self.endpoint = endpoint
        self.access_key = "minioadmin"
        self.secret_key = "minioadmin"
        self.bucket_name = bucket_name
        self.object_prefix = object_prefix

    def process(self, element):
        table = pa.Table.from_pylist(element)
        with BytesIO() as buffer:
            pq.write_table(table, buffer, compression='snappy')
            buffer.seek(0)
            minio_client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=False
            )
            
            object_name = f"{self.object_prefix}/{uuid.uuid4()}.parquet"

            minio_client.put_object(
                self.bucket_name,
                object_name,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
        logging.info(f"Written to MinIO: {object_name}")
        yield element

def run():
    endpoint = "minio:9000"
    bucket_name = "input"
    input_prefix = "data"
    output_prefix = "filtered"
    access_key = "minioadmin" 
    secret_access_key = "minioadmin"
    input_path = f's3://{bucket_name}/{input_prefix}/*.parquet'

    batch_size = 100000

    options = PipelineOptions([
        f'--s3_access_key_id={access_key}',
        f'--s3_secret_access_key={secret_access_key}',
        f'--s3_endpoint_url=http://{endpoint}',
    ])

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadParquet' >> parquetio.ReadFromParquet(input_path)
            | 'FilterEurope' >> beam.Filter(lambda elem: elem.get('continent_exp', '') == 'Europe')
            | 'Batch elevemts' >> beam.BatchElements(min_batch_size=100000, max_batch_size=1000000)
            | 'WriteToMinio' >> beam.ParDo(WriteToMinio(endpoint, bucket_name, output_prefix))
        )

if __name__ == "__main__":
    run()