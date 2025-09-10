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
    'beam_to_filter_data',
    default_args=default_args,
    description='Run Beam to filter data in minio',
    schedule_interval='@daily',
    catchup=False,
)

run_filter_data = DockerOperator(
    task_id='run_filter',
    image='af_beam_image:latest',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=[
        'python', 'beam_filter.py',
        '--minio_endpoint', 'minio:9000',
        '--minio_bucket', 'input',
        '--minio_input_prefix', 'data',
        '--minio_output_prefix', 'filtered',
        '--minio_access_key', 'minioadmin',
        '--minio_secret_key', 'minioadmin',
        '--batch_size', '100000'
    ],
    docker_url='unix://var/run/docker.sock',
    network_mode='my_network',
    dag=dag,
)

run_filter_data
