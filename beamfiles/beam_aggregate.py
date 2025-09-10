import apache_beam as beam
from apache_beam.io import parquetio
from apache_beam.options.pipeline_options import PipelineOptions
import os
import sys
from typing import NamedTuple, Optional
from apache_beam import CoGroupByKey
from helper.functions import WriteToMinio, CustomOptions
import logging


class Record(NamedTuple):
    year: Optional[int]
    month: Optional[str]
    country_territory_code: Optional[str]
    cases: Optional[int]
    deaths: Optional[int]

def dict_to_namedtuple(d):
    return Record(
        year=d['year'] or 0,
        month = d['month'] or None,
        country_territory_code = d['country_territory_code'] or None,
        cases=d['cases'] or 0,
        deaths=d['deaths'] or 0
    )

def run(argv=None):
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    logger = logging.getLogger(__name__)

    options = PipelineOptions(argv)
    options = PipelineOptions(flags=argv)
    custom_options = options.view_as(CustomOptions)

    bucket_name = custom_options.minio_bucket
    endpoint = custom_options.minio_endpoint
    input_prefix = custom_options.minio_input_prefix
    output_prefix = custom_options.minio_output_prefix
    access_key = custom_options.minio_access_key
    secret_key = custom_options.minio_secret_key
    batch_size = custom_options.batch_size

    input_path = f's3://{bucket_name}/{input_prefix}/*.parquet'

    options = PipelineOptions([
        f'--s3_access_key_id={access_key}',
        f'--s3_secret_access_key={secret_key}',
        f'--s3_endpoint_url=http://{endpoint}',
    ])

    logger.info(f"Parameters: minio_endpoint={endpoint}, minio_bucket={bucket_name}, minio_prefix={input_prefix}, batch_size={batch_size}")

    with beam.Pipeline(options=options) as p:
        data = (
            p
            | 'ReadParquet' >> parquetio.ReadFromParquet(input_path)
            | 'ToBeamRows' >> beam.Map(dict_to_namedtuple)
            | 'Filter' >> beam.Filter(lambda elem: elem.country_territory_code in ['GBR', 'NLD'])
            | 'Agg' >> beam.GroupBy('year', 'country_territory_code', 'month')\
                    .aggregate_field('cases', sum, 'sum_cases')\
                    .aggregate_field('deaths', sum, 'sum_deaths')
        )

        gb_data = (
            data 
            | 'filter gb' >> beam.Filter(lambda x: x.country_territory_code == 'GBR')
            | 'beam map gbr' >> beam.Map(lambda x: ((x.year, x.month), x))
        )

        nl_data = (
            data 
            | 'filter nl' >> beam.Filter(lambda x: x.country_territory_code == 'NLD')
            | 'beam map nld' >> beam.Map(lambda x: ((x.year, x.month), x))
        )

        joined = (
        {'gb': gb_data, 'nl': nl_data}
        | 'CoGroupByYear' >> CoGroupByKey()
        | 'ProcessJoined' >> beam.FlatMap(
                lambda kv: [
                    {
                        'year': kv[0][0],
                        'month': kv[0][1],
                        'gb_cases': gb.sum_cases,
                        'gb_death': gb.sum_deaths,
                        'nl_cases': nl.sum_cases,
                        'nl_death': nl.sum_deaths
                    }
                    for gb in kv[1]['gb']
                    for nl in kv[1]['nl']
                ]
            )
        )
    
        w = ( 
            joined
            | 'Batch elevemts' >> beam.BatchElements(min_batch_size=batch_size, max_batch_size=batch_size * 100)
            | 'WriteToMinio' >> beam.ParDo(WriteToMinio(endpoint, bucket_name, output_prefix, access_key, secret_key))
        )

    logger.info("Pipeline completed successfully.")

if __name__ == '__main__':
    run()
