import requests
import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "retries": 1,
}


@dag(
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    dag_id="load_sentiment",
    params={
        "limit": 1_000,
    }
)
def load_sentiment_dag(**context):
    @task
    def create_sentiment_table():
        hook = PostgresHook(postgres_conn_id="postgres")
        sql = """
            CREATE TABLE IF NOT EXISTS sentiment_data (
                value INTEGER NOT NULL,
                value_classification TEXT NOT NULL,
                timestamp BIGINT NOT NULL,
                PRIMARY KEY (timestamp)
            );
        """
        hook.run(sql)
    

    @task
    def create_sentiment_view():
        hook = PostgresHook(postgres_conn_id="postgres")
        sql = """
            CREATE OR REPLACE VIEW sentiment_data__view AS
            SELECT
                to_timestamp(timestamp) AS "TimeStamp",
                value AS "Value",
                value_classification AS "Class"
            FROM sentiment_data;
        """
        hook.run(sql)
    

    @task
    def load_sentiment(**context):
        limit = context["params"].get("limit")
        df = pd.DataFrame(requests.get(f"https://api.alternative.me/fng/?limit={limit}").json()["data"])
        df = df[df["time_until_update"].isna()]
        df.drop(
            columns=["time_until_update"],
            inplace=True
        )
        
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.insert_rows("sentiment_data", df.itertuples(index=False), target_fields=df.columns.tolist())


    create_bars_table_task = create_sentiment_table()
    create_bars_view_task = create_sentiment_view()
    load_sentiment_task = load_sentiment()

    create_bars_table_task >> create_bars_view_task >> load_sentiment_task


load_sentiment = load_sentiment_dag()