all: down init up

down:
	@echo "Stopping Airflow services..."
	@docker compose down
	@echo "Completed."
	@echo "Removing previous session's PostgreSQL data..."
	@docker volume rm airflow-backtesting_postgres-db-volume
	@echo "Completed."

init:
	@echo "Removing previous session's logs, plugins and config directories."
	@rm -rf ./logs ./plugins ./config
	@echo "Completed."
	@echo "Ensuring proper file structure..."
	@mkdir -p ./logs ./plugins ./config
	@echo "Setting appropriate permissions..."
	@chmod -R 777 ./dags ./logs ./plugins ./config
	@echo "Initializing Airflow (if necessary)..."
	@docker compose up airflow-init

up:
	@echo "Starting Airflow services..."
	@docker compose up -d
	@echo "Completed."

.PHONY: all down init up
