from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'beam_to_load_input_external',
    default_args=default_args,
    description='Run Beam to load external data to minio',
    schedule_interval='@daily',
    catchup=False,
)

run_beam_external = DockerOperator(
    task_id='run_beam_read_external_parquet',
    image='af_beam_image:latest',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=[
        'python', 'beam_input.py',
        '--parquet_url', 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.parquet',
        '--minio_endpoint', 'minio:9000',
        '--minio_bucket', 'input',
        '--minio_output_prefix', 'data',
        '--minio_access_key', 'minioadmin',
        '--minio_secret_key', 'minioadmin',
        '--batch_size', '100000'
    ],
    docker_url='unix://var/run/docker.sock',
    network_mode='my_network',
    dag=dag,
)

run_beam_external
