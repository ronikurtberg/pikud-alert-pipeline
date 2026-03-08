.PHONY: run test test-fast fetch rebuild validate status docker help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

run: ## Start the dashboard server
	python3 dashboard.py

fetch: ## First run: full download + DB build. After: fetch new alerts only
	@if [ -d data ] && [ "$$(ls -A data 2>/dev/null)" ]; then \
		python3 pikud.py delta; \
	else \
		python3 pikud.py full_refresh; \
	fi

rebuild: ## Rebuild DB from CSVs
	python3 pikud.py rebuild_db

validate: ## Run pipeline validation
	python3 pikud.py validate

status: ## Show pipeline status
	python3 pikud.py status

test: ## Run all tests
	python3 -m pytest tests/ -v

test-fast: ## Run parser + API tests only
	python3 -m pytest tests/test_parsers.py tests/test_api.py -v

test-perf: ## Run performance budget tests
	python3 -m pytest tests/test_performance.py -v

restart: ## Restart the dashboard
	pkill -f "python3.*dashboard.py" 2>/dev/null; sleep 1; python3 dashboard.py &

install: ## Install dependencies
	pip3 install -r requirements.txt

wifi: ## Start on WiFi (accessible from phones on same network)
	@IP=$$(ipconfig getifaddr en0 2>/dev/null || echo "check-ip"); \
	echo "Mobile: http://$$IP:5000/summary"; \
	python3 dashboard.py

tunnel: ## Expose to internet via ngrok (requires: ngrok authtoken YOUR_TOKEN)
	ngrok http 5000

docker: ## Build and run with Docker
	docker-compose up --build
