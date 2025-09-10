from apache_beam.transforms import DoFn
from io import BytesIO
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import uuid
import requests
from apache_beam.transforms import DoFn
from apache_beam.options.pipeline_options import PipelineOptions


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--parquet_url', type=str, help='URL for parquet file')
        parser.add_argument('--minio_endpoint', type=str, default='minio:9000', help='MinIO Endpoint')
        parser.add_argument('--minio_bucket', type=str, default='input', help='MinIO bucket name')
        parser.add_argument('--minio_input_prefix', type=str, default='data', help='MinIO input prefix')
        parser.add_argument('--minio_output_prefix', type=str, default='filtered', help='MinIO output prefix')
        parser.add_argument('--minio_access_key', type=str, default='minioadmin', help='MinIO access key')
        parser.add_argument('--minio_secret_key', type=str, default='minioadmin', help='MinIO secret key')
        parser.add_argument('--batch_size', type=int, default=100000, help='Batch size')
        

class WriteToMinio(DoFn):
    def __init__(self, endpoint: str, bucket_name: str, object_prefix: str, access_key: str, secret_key:str):
        self.minio_client = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
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


class DownloadExternalParquetFile(DoFn):
    def __init__(self, url: str):
        self.url = url

    def process(self, element):
        response = requests.get(self.url)
        response.raise_for_status()
        table = pq.read_table(BytesIO(response.content))
        yield from table.to_pylist()