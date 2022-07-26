help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
.DEFAULT_GOAL := help
.SILENT: start stop query kpi restart

start: ## Start the application and seed the data.
	docker-compose up -d 2>/dev/null
	docker-compose exec spark spark-submit /work/jobs/main.py seed

stop: ## Stop the application.
	docker-compose down -v 2>/dev/null

restart: ## Restart the application and seed the data.
	docker-compose down -v 2>/dev/null
	docker-compose up -d 2>/dev/null
	docker-compose exec spark spark-submit /work/jobs/main.py seed

query: ## Get data from query.
	docker-compose exec spark spark-submit /work/jobs/main.py run_query --query="$(query)"

kpi: ## Get data for KPI.
	docker-compose exec spark spark-submit /work/jobs/main.py get_kpi --kpi="$(kpi)" --district_name="$(district_name)" --top="$(top)"