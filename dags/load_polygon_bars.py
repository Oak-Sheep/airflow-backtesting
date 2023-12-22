import requests
import pandas as pd
from datetime import (
    date,
    datetime
)

from airflow.decorators import dag, task
from airflow.models import Variable
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
    dag_id="load_polygon_bars",
    params={
        "symbols": [
            "X:BTCUSD",
            "X:ETHUSD",
            "X:1INCHUSD",
            "X:XRPUSD",
            "X:ADAUSD"
        ],
        "start_date": "2022-01-01",
    }
)
def load_polygon_bars_dag(**context):
    @task
    def create_bars_table():
        hook = PostgresHook(postgres_conn_id="postgres")
        sql = """
            CREATE TABLE IF NOT EXISTS bars_data (
                s TEXT NOT NULL,
                t BIGINT NOT NULL,
                o NUMERIC NOT NULL,
                h NUMERIC NOT NULL,
                l NUMERIC NOT NULL,
                c NUMERIC NOT NULL,
                v NUMERIC NOT NULL,
                vw NUMERIC NOT NULL,
                n BIGINT NOT NULL,
                otc BOOL NOT NULL DEFAULT FALSE,
                PRIMARY KEY (s, t)
            );
        """
        hook.run(sql)


    @task
    def create_bars_view():
        hook = PostgresHook(postgres_conn_id="postgres")
        sql = """
            CREATE OR REPLACE VIEW bars_data__view AS
            SELECT
                s AS "Symbol",
                to_timestamp(t/1000) AS "TimeStamp",
                o AS "Open",
                h AS "High",
                l AS "Low",
                c AS "Close",
                v AS "Volume",
                vw AS "VWAP",
                n AS "OpenInterest",
                otc AS "IsOTC"
            FROM bars_data;
        """
        hook.run(sql)


    @task
    def load_symbols(**context):
        return context["params"].get("symbols")


    @task
    def load_bars(symbol: str, **context):
        start_date = context["params"].get("start_date")
        end_date = date.today().strftime(r"%Y-%m-%d")
        api_key = Variable.get("polygon_token")
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"
        response = requests.get(url)
        data = response.json()

        df = pd.DataFrame(data["results"])
        df["s"] = symbol

        hook = PostgresHook(postgres_conn_id="postgres")
        hook.insert_rows("bars_data", df.itertuples(index=False), target_fields=df.columns.tolist())


    create_bars_table_task = create_bars_table()
    create_bars_view_task = create_bars_view()

    load_bars_task = load_bars.expand(symbol=load_symbols())

    create_bars_table_task >> create_bars_view_task >> load_bars_task


load_polygon_bars = load_polygon_bars_dag()