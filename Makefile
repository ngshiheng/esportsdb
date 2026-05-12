NAME := esportsdb

SHELL=/bin/bash
DATASETTE := $(shell command -v datasette 2> /dev/null)
UV := $(shell command -v uv 2> /dev/null)
SQLITE_FILE = data/esports.db

.DEFAULT_GOAL := help
##@ Helper
.PHONY: help
help:   ## display this help message.
	@echo "Welcome to $(NAME) [$(ENVIRONMENT)]."
	@awk 'BEGIN {FS = ":.*##"; printf "Use make \033[36m<target>\033[0m where \033[36m<target>\033[0m is one of:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Usage
.PHONY: run
run:    ## run scraper.
	@$(UV) run --script scrape.py --db $(SQLITE_FILE)

.PHONY: test
test:   ## run unit tests.
	@$(UV) run --script test_scrape.py -v

.PHONY: inspect
inspect:    ## generate inspect file for performance optimization.
	@[ -f $(SQLITE_FILE) ] && echo "File $(SQLITE_FILE) exists." || { echo "File $(SQLITE_FILE) does not exist." >&2; exit 1; }
	@$(DATASETTE) inspect $(SQLITE_FILE) --inspect-file=data/inspect.json
	@echo "Generated inspect file at data/inspect.json"

.PHONY: datasette
datasette:  ## run datasette with optimizations.
	@[ -f $(SQLITE_FILE) ] && echo "File $(SQLITE_FILE) exists." || { echo "File $(SQLITE_FILE) does not exist." >&2; exit 1; }
	@if [ -z $(DATASETTE) ]; then echo "Datasette could not be found. See https://docs.datasette.io/en/stable/installation.html"; exit 2; fi
	@if [ ! -f data/inspect.json ]; then $(MAKE) inspect; fi
	@$(DATASETTE) -i $(SQLITE_FILE) --inspect-file=data/inspect.json --setting allow_download off --setting allow_csv_stream off --setting max_csv_mb 1 --setting default_cache_ttl 86400 --setting sql_time_limit_ms 2000 --metadata data/metadata.json --root

##@ Docker
IMAGE_NAME := ngshiheng/esportsdb
TAG_DATE := $(shell date -u +%Y%m%d)

##@ Contributing
.PHONY: setup-dev
setup-dev: ## install development dependencies including required Datasette plugins.
	@if [ -z $(DATASETTE) ]; then echo "Installing Datasette..."; pip install datasette; fi
	@echo "Installing required Datasette plugins..."
	@pip install datasette-block-robots datasette-vega datasette-gzip datasette-google-analytics kaggle
	@echo "Setup complete! Run 'make datasette' to start local development server."
