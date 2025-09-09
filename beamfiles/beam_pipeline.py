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
from minio.error import S3Error
from io import BytesIO
from apache_beam.transforms import DoFn
import logging
import uuid
from datetime import datetime

# provide simple template for apache beam pipeline which reads from minio parquet files and simple filter it and put to target same conainer
# can you directly read data from minio without storing in temp directory? apply filters and bulk write to minio?

class FilterAndTransform(beam.DoFn):
    def process(self, element):
        if element.get('continent_exp', '') ==  'Europe':
            element['processed_at'] = datetime.utcnow().isoformat()
            yield element
        else:
            pass

class WriteToMinio(DoFn):
    def __init__(self, endpoint: str, bucket_name: str, object_prefix: str):
        self.minio_client = endpoint
        self.access_key = "minioadmin"
        self.secret_key = "minioadmin"
        self.bucket_name = bucket_name
        self.object_prefix = object_prefix

    def process(self, element):
        table = pa.Table.from_pylist(element)
        with BytesIO() as buffer:
            pq.write_table(table, buffer)
            buffer.seek(0)
            minio_client = Minio(
                self.minio_client,
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
    options = PipelineOptions(
        runner='DirectRunner',
    )

    minio_client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "input"
    input_prefix = "data/"
    output_prefix = "filtered/"

    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    with beam.Pipeline(options=options) as p:
        input_files = p | 'Create list' >> beam.Create(
            [f"s3://{bucket_name}/{obj.object_name}" for obj in minio_client.list_objects(bucket_name, prefix=input_prefix)]
        )

        records  = (input_files
         | 'ReadFromMinio' >> parquetio.ReadAllFromParquet()
         | 'FilterAndTransform' >> beam.ParDo(FilterAndTransform())
         | 'WriteToMinio' >> beam.ParDo(WriteToMinio("localhost:9000", bucket_name, output_prefix))
        )


if __name__ == "__main__":
    run()