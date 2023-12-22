# Airflow Backtesting

Includes:
+ Crypto DataFeed (Polygon IO, AlternativeMe)
+ Backtesting (via Backtesting.py module)
+ Two trading strategies: SMA and LOGIT

ENV variables are required:
+ AIRFLOW_UID
+ AIRFLOW_GID
+ AIRFLOW_CONN_POSTGRES
+ AIRFLOW_VAR_POLYGON_TOKEN
+ AIRFLOW_SMTP_USER
+ AIRFLOW_SMTP_PASSWORD
+ AIRFLOW_SMTP_EMAIL
+ AIRFLOW_EMAIL_LIST

Architecture:
![Alt text](/diagram/airflow_backtesting.png)