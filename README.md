# Airflow Backtesting

Includes:
+ Crypto DataFeed (Polygon IO, AlternativeMe)
+ Backtesting (via Backtesting.py module)
+ Two trading strategies: SMA and LOGIT

Architecture:
![Alt text](/diagram/airflow_backtesting.png)

Launch service:
```sudo make init
make up
```

Shutdown service:```make down```



ENV variables are required:
- `AIRFLOW_UID`: Идентификатор пользователя Airflow
- `AIRFLOW_GID`: Идентификатор группы для Airflow
- `AIRFLOW_CONN_POSTGRES`: Строка подключения к базе данных PostgreSQL для Airflow
- `AIRFLOW_VAR_POLYGON_TOKEN`: Токен для доступа к API Polygon
- `AIRFLOW_SMTP_USER`: Имя пользователя для SMTP-сервера, используемого для отправки электронной почты
- `AIRFLOW_SMTP_PASSWORD`: Пароль для SMTP-сервера
- `AIRFLOW_SMTP_EMAIL`: Адрес электронной почты, с которого будут отправляться уведомления
- `AIRFLOW_EMAIL_LIST`: Список адресов электронной почты для уведомлений, разделенных запятыми