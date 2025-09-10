# I want to create pipeline with apache beam that took parquet by specific URL and put into minio container in my local docker
# so could you implement the whole code for me?
# parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
#         logger.info("No parquet files found in the source bucket.")
#         for parquet_file in parquet_files:
#             local_path = os.path.join(temp_dir, os.path.basename(parquet_file))
#             minio_client.fget_object(source_bucket, parquet_file, local_path)
#             logger.info(f"Downloaded {parquet_file} to {local_path}")
#             local_parquet_files.append(local_path)
#
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from io import BytesIO
from apache_beam.transforms import DoFn, PTransform
from apache_beam.typehints import with_input_types, with_output_types, Dict, Any
import uuid
import sys
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import apache_beam as beam

class DownloadParquetFile(DoFn):
    def __init__(self, url: str):
        self.url = url

    def process(self, element):
        response = requests.get(self.url)
        response.raise_for_status()
        table = pq.read_table(BytesIO(response.content))
        yield from table.to_pylist()

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
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    logger = logging.getLogger(__name__)

    parquet_url = "https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.parquet"
    endpoint = "minio:9000"
    bucket_name = "input"
    object_prefix = "data"
    batch_size = 100000
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Start" >> beam.Create([None])
            | "Download Parquet" >> beam.ParDo(DownloadParquetFile(parquet_url))
            | 'Batch elevemts' >> beam.BatchElements(min_batch_size=batch_size, max_batch_size=batch_size * 100)
            | "Write to MinIO" >> beam.ParDo(WriteToMinio(endpoint, bucket_name, object_prefix))
        )
    logger.info("Pipeline completed successfully.")

if __name__ == "__main__":
    run()