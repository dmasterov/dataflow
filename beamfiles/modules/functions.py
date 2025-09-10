from apache_beam.transforms import DoFn
from io import BytesIO
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import uuid


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