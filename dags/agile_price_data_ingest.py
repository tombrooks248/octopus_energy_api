import os
from datetime import datetime, timedelta
import requests

import sqlalchemy as sa
from pg_bulk_ingest import ingest

from airflow.datasets import Dataset
from airflow.decorators import dag, task

import logging
_logger = logging.getLogger('pg_bulk_ingest')
_logger = _logger.setLevel('WARNING')

# 1. Define the function that makes up the single task of this DAG

@task(
    retries=1,
    retry_delay=timedelta(seconds=5),
)
def sync(
    dag_id,
    schema,
    table_name,
    high_watermark,
    delete,
):
    engine = sa.create_engine(
        url=os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'],
        future=True
    )

    if engine.url == os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN']:
        _logger.warning('''
            You are ingesting into Airflow\'s metadata database.
            You should only do this for the pg_bulk_ingest test case.
            Change the engine url to match your production database.
        ''')

    # The SQLAlchemy definition of the table to ingest data into
    metadata = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata,
        sa.Column("valid_from", sa.VARCHAR(20), primary_key=True),
        sa.Column("valid_to", sa.VARCHAR(20)),
        sa.Column("price_exc_vat", sa.DECIMAL, nullable=False),
        sa.Index(None, "valid_from"),
        schema=schema,
    )

    def batches(high_watermark_value):

        price_start_datetime="2024-12-28T00:00Z"
        price_end_datetime= datetime.now().strftime("%Y-%m-%dT%H:%MZ")

        logging.info(price_start_datetime)
        logging.info(price_end_datetime)

        data = []

        url = "lets go"

        url = f"https://api.octopus.energy/v1/products/AGILE-FLEX-22-11-25/electricity-tariffs/E-1R-AGILE-FLEX-22-11-25-C/standard-unit-rates/?period_from={price_start_datetime}&period_to={price_end_datetime}"

        while url != None:
            response = requests.get(url).json()
            batch_data = response["results"]
            for row in batch_data:
                data.append(row)
            url = response["next"]
            logging.info(url)


        logging.info(len(data))
        logging.info(data[0])


        tuples = []
        for row in data:
            tuples.append((row["valid_from"], row["valid_to"], row["value_exc_vat"]))

        # No data for now

        yield None, None, (
         (table, t) for t in tuples
      )

    def on_before_visible(
        conn, ingest_table, source_modified_date
    ):
        pass

    with engine.connect() as conn:
        ingest(
             conn,
             metadata,
             batches,
             on_before_visible=on_before_visible,
             high_watermark=high_watermark,
             delete=delete,
        )


# 2. Define the function that creates the DAG from the single task

def create_dag(dag_id, schema, table_name):
    @dag(
        dag_id=dag_id,
        start_date=datetime(2022, 5, 16),
        schedule='@daily',
        catchup=False,
        max_active_runs=1,
        params={
            'high_watermark': '__LATEST__',
            'delete': '__BEFORE_FIRST_BATCH__',
        },
    )
    def Pipeline():
        sync.override(
            outlets=[
                Dataset(f'postgresql://datasets//{schema}/{table_name}'),
            ]
        )(
            dag_id=dag_id,
            schema=schema,
            table_name=table_name,
            high_watermark="",
            delete="",
        )

    Pipeline()

# 3. Call the function that creates the DAG

create_dag('uk_agile_prices', 'public', 'uk_power_prices_agile')
