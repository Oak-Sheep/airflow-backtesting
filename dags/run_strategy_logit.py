import pandas as pd
from datetime import (
    date,
    datetime
)
from backtesting import (
    Backtest,
    Strategy
)
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler


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


LOGIT_FEATURES = [
    "Return",
    "FG",
    "USDPerInterest"
]


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
                "Volume",
                "OpenInterest",
                "VWAP",
                "Value" AS "FG",
                ROUND("Close" / "Open" - 1, 2) AS "Return",
                ROUND("Volume" * "VWAP" / "OpenInterest") AS "USDPerInterest"
            FROM bars_data__view
            JOIN sentiment_data__view
                USING ("TimeStamp")
            WHERE 1=1
                AND "Symbol" = '{symbol}'
                AND "TimeStamp" >= '{start_date}'
                AND "TimeStamp" <= '{end_date}'
        """,
        con=conn
    )
    data.set_index("TimeStamp", inplace=True)

    data["Target"] = (data["Close"] > data["Open"]).astype(int)
    data["Target"] = data["Target"].shift(-1)

    scaler = StandardScaler()
    data[LOGIT_FEATURES] = scaler.fit_transform(data[LOGIT_FEATURES])
    data.dropna(inplace=True)

    return data


class Logit(Strategy):
    window = 0

    def init(self):
        self.logistic_model = LogisticRegression()


    def next(self):
        start = max(0, len(self.data) - self.window)
        end = len(self.data)

        if end - start < 30:
            return

        train_data = self.data.df.iloc[start:end]
        X = train_data.loc[:, LOGIT_FEATURES]
        y = train_data["Target"]
        self.logistic_model.fit(X, y)

        pred = self.logistic_model.predict(
            [[self.data[i][-1] for i in LOGIT_FEATURES]]
        )[0]
        if pred == 1 and not self.position:
            self.buy()
        elif pred == 0 and not self.position:
            self.sell()
        elif pred == 1 and self.position.size < 0:
            self.position.close()
            self.buy()
        elif pred == 0 and self.position.size > 0:
            self.position.close()
            self.sell()


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    dag_id="run_strategy_logit",
    params={
        "symbol": "X:BTCUSD",
        "start_date": "2022-01-01",
        "end_date": date.today().strftime(r"%Y-%m-%d"),
        "window": 65,
        "cash": 100_000,
        "commission": 0.002
    }
)
def logit_backtest_dag(**context):
    @task
    def run_backetst(**context):
        params = context["params"]
        symbol = params.get("symbol")
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        window = params.get("window")
        cash = params.get("cash")
        commission = params.get("commission")
        commission = float(commission)

        parameters = {
            "Symbol": symbol,
            "Window": window,
            "Cash": cash,
            "Commission": commission,
        }

        strategy = Logit
        strategy.window = window

        data = load_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )

        bt = Backtest(
            data=data,
            strategy=Logit,
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


dag_instance = logit_backtest_dag()