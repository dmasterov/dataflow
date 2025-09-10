FROM python:3.10-slim

WORKDIR /app

COPY . /app

# Install additional dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the entrypoint for the container to run the Beam script easily
CMD ["python"]

# docker build -t af_beam_image .

# docker run --rm --network my_network af_beam_image:latest \
# python beam_input.py --parquet_url https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.parquet --minio_endpoint minio:9000 --minio_bucket input --minio_prefix data --minio_access_key minioadmin --minio_secret_key minioadmin --batch_size 100000
