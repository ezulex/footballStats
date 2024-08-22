import pandas as pd
from airflow.models import DAG
from datetime import datetime
import logging
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.decorators import task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
import custom.football_stats_to_s3_functions as ff

S3_ACCESS_KEY = "{{ conn.minio.login }}"
S3_SECRET_KEY = "{{ conn.minio.password }}"
S3_WORK_BUCKET = "fstats"
S3_ENDPOINT = "{{ conn.minio.host }}"
DBT_START = Dataset("greenplum://create_ext")


@task(task_id="create_bucket")
def create_bucket(bucket_name, login, secret, endpoint, ds_nodash=None):
    ff.create_bucket(bucket_name, login, secret, endpoint, ds_nodash)


@task(task_id="load_competitions_to_s3")
def load_competitions_to_s3(bucket_name, login, secret, endpoint, ds_nodash=None):
    ff.load_competitions_to_s3(bucket_name, login, secret, endpoint, ds_nodash)


@task(task_id="load_matches_to_s3")
def load_matches_to_s3(bucket_name, login, secret, endpoint, ds_nodash=None):
    ff.load_matches_to_s3(bucket_name, login, secret, endpoint, ds_nodash)


@task(task_id="load_managers_to_s3")
def load_managers_to_s3(bucket_name, login, secret, endpoint, ds_nodash=None):
    ff.load_managers_to_s3(bucket_name, login, secret, endpoint, ds_nodash)


@task(task_id="load_lineups_to_s3")
def load_lineups_to_s3(bucket_name, login, secret, endpoint, ds_nodash=None):
    ff.load_lineups_to_s3(bucket_name, login, secret, endpoint, ds_nodash)


@task(task_id="load_cards_to_s3")
def load_cards_to_s3(bucket_name, login, secret, endpoint, ds_nodash=None):
    ff.load_cards_to_s3(bucket_name, login, secret, endpoint, ds_nodash)


@task(task_id="load_positions_to_s3")
def load_positions_to_s3(bucket_name, login, secret, endpoint, ds_nodash=None):
    ff.load_positions_to_s3(bucket_name, login, secret, endpoint, ds_nodash)


with DAG(
        dag_id="football_stats_raw",
        start_date=datetime(2024, 7, 30),
        schedule="30 10 * * *",  #@daily
        catchup=False
) as dag:
    start_loading = EmptyOperator(task_id="start")
    finish_loading = EmptyOperator(task_id="finish", outlets=[DBT_START])

    create_bucket = create_bucket(S3_WORK_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)


    #КАК ИСПОЛЬЗОВАТЬ
    # load_lineups = []

    @task_group(group_id="data_to_s3")
    def data_to_s3():
        load_competitions = load_competitions_to_s3(S3_WORK_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)
        load_matches = load_matches_to_s3(S3_WORK_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)
        load_managers = load_managers_to_s3(S3_WORK_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)
        load_lineups = load_lineups_to_s3(S3_WORK_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)
        load_cards = load_cards_to_s3(S3_WORK_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)
        load_positions = load_positions_to_s3(S3_WORK_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)


    @task_group(group_id="export_to_gp")
    def export_to_gp():
        load_competitions_to_gp = PostgresOperator(
            task_id="load_competitions_to_gp",
            postgres_conn_id="Greenplum",
            sql='sql/competitions_to_gp.sql',
            params={"bucket": S3_WORK_BUCKET},
        )

        load_lineups_to_gp = PostgresOperator(
            task_id="load_lineups_to_gp",
            postgres_conn_id="Greenplum",
            sql='sql/lineups_to_gp.sql',
            params={"bucket": S3_WORK_BUCKET},
        )

        load_matches_to_gp = PostgresOperator(
            task_id="load_matches_to_gp",
            postgres_conn_id="Greenplum",
            sql='sql/matches_to_gp.sql',
            params={"bucket": S3_WORK_BUCKET},
        )

        load_managers_to_gp = PostgresOperator(
            task_id="load_managers_to_gp",
            postgres_conn_id="Greenplum",
            sql='sql/managers_to_gp.sql',
            params={"bucket": S3_WORK_BUCKET},
        )

        load_cards_to_gp = PostgresOperator(
            task_id="load_cards_to_gp",
            postgres_conn_id="Greenplum",
            sql='sql/cards_to_gp.sql',
            params={"bucket": S3_WORK_BUCKET},
        )

        load_positions_to_gp = PostgresOperator(
            task_id="load_positions_to_gp",
            postgres_conn_id="Greenplum",
            sql='sql/positions_to_gp.sql',
            params={"bucket": S3_WORK_BUCKET},
        )

        load_competitions_to_gp
        load_lineups_to_gp
        load_matches_to_gp
        load_managers_to_gp
        load_cards_to_gp
        load_positions_to_gp


    start_loading >> create_bucket >> data_to_s3() >> export_to_gp() >> finish_loading
