import apache_beam as beam
from apache_beam.io import parquetio
from apache_beam.options.pipeline_options import PipelineOptions
import os
from typing import NamedTuple, Optional
from apache_beam import CoGroupByKey
from modules.functions import WriteToMinio


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

def run():
    input_path = 's3://input/filtered/*.parquet'
    endpoint = "minio:9000"
    bucket_name = "input"
    output_prefix = "aggregate"

    # Beam pipeline options
    options = PipelineOptions([
        '--s3_access_key_id=minioadmin',
        '--s3_secret_access_key=minioadmin',
        '--s3_endpoint_url=http://minio:9000',
    ])

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
            | 'Batch elevemts' >> beam.BatchElements(min_batch_size=100000, max_batch_size=1000000)
            | 'WriteToMinio' >> beam.ParDo(WriteToMinio(endpoint, bucket_name, output_prefix))
        )

if __name__ == '__main__':
    run()
