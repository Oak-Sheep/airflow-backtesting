import pandas as pd
from datetime import (
    date,
    datetime
)
from backtesting import (
    Backtest,
    Strategy
)
from backtesting.test import SMA
from backtesting.lib import crossover


from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email_operator import EmailOperator


from utils import *


default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "retries": 1,
}


def load_data(
    symbol: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    data = pd.read_sql(
        sql=f"""
            SELECT 
                "TimeStamp",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume"
            FROM bars_data__view
            WHERE 1=1
                AND "Symbol" = '{symbol}'
                AND "TimeStamp" >= '{start_date}'
                AND "TimeStamp" <= '{end_date}'
        """,
        con=conn
    )
    data.set_index("TimeStamp", inplace=True)
    return data


class SmaCross(Strategy):
    def init(self):
        close = self.data.Close
        self.sma1 = self.I(SMA, close, self.n1)
        self.sma2 = self.I(SMA, close, self.n2)

    def next(self):
        if crossover(self.sma1, self.sma2):
            self.buy()
        elif crossover(self.sma2, self.sma1):
            self.sell()


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    dag_id="run_strategy_sma",
    params={
        "symbol": "X:BTCUSD",
        "start_date": "2022-01-01",
        "end_date": date.today().strftime(r"%Y-%m-%d"),
        "fast_window": 5,
        "slow_window": 20,
        "cash": 100_000,
        "commission": 0.002
    }
)
def sma_cross_backtest_dag(**context):
    @task
    def run_backetst(**context):
        params = context["params"]
        symbol = params.get("symbol")
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        fast_window = params.get("fast_window")
        slow_window = params.get("slow_window")
        cash = params.get("cash")
        commission = params.get("commission")
        commission = float(commission)

        parameters = {
            "Symbol": symbol,
            "Fast Window": fast_window,
            "Slow Window": slow_window,
            "Cash": cash,
            "Commission": commission,
        }

        strategy = SmaCross
        strategy.n1 = fast_window
        strategy.n2 = slow_window

        data = load_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )

        bt = Backtest(
            data=data,
            strategy=SmaCross,
            cash=cash,
            commission=commission,
            exclusive_orders=True
        )
        stats = bt.run()

        html_content = html_report(
            parameters=parameters,
            stats=stats
        )

        return html_content

    html_content = run_backetst()

    send_email = EmailOperator(
        task_id="send_email",
        to=Variable.get("email_list").split(","),
        subject="Run Strategy Report",
        html_content=html_content,
    )


dag_instance = sma_cross_backtest_dag()