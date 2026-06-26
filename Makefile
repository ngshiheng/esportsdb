NAME := esportsdb

SHELL=/bin/bash
DATASETTE := $(shell command -v datasette 2> /dev/null)
DOCKER := $(shell command -v docker 2> /dev/null)
UV := $(shell command -v uv 2> /dev/null)
SQLITE_FILE = data/esports.db

IMAGE_NAME := ngshiheng/esportsdb
TAG_DATE := $(shell date -u +%Y%m%d)

.DEFAULT_GOAL := help
##@ Helper
.PHONY: help
help:   ## display this help message.
	@echo "Welcome to $(NAME)."
	@awk 'BEGIN {FS = ":.*##"; printf "Use make \033[36m<target>\033[0m where \033[36m<target>\033[0m is one of:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Usage
.PHONY: setup
setup:    ## setup scraper.
	@$(UV) venv --clear
	@$(UV) export --script scrape.py | $(UV) pip sync -

.PHONY: run
run: scrape-fast    ## run scraper (alias for scrape-fast; use scrape-slow for first-time backfill).

.PHONY: scrape-fast
scrape-fast:    ## refresh running/upcoming data + teams (mirrors scrape-fast.yml).
	@$(UV) run --script scrape.py --db $(SQLITE_FILE) \
        --resource series_upcoming --resource series_running \
        --resource tournaments_upcoming --resource tournaments_running \
        --resource matches_upcoming --resource matches_running \
        --resource teams

.PHONY: scrape-slow
scrape-slow:    ## refresh reference data — leagues, series, tournaments, teams, players (mirrors scrape-daily.yml).
	@$(UV) run --script scrape.py --db $(SQLITE_FILE) \
		--resource videogames --resource leagues \
		--resource series --resource tournaments \
		--resource teams --resource players

.PHONY: scrape-history
scrape-history:    ## one-time full historical match backfill (slow, hours — run manually once).
	@$(UV) run --script scrape.py --db $(SQLITE_FILE) --resource matches --page-delay 3

.PHONY: test
test:   ## run unit tests.
	@$(UV) run --script test_scrape.py -v

.PHONY: datasette
datasette:  ## run datasette with optimizations.
	@[ -f $(SQLITE_FILE) ] && echo "File $(SQLITE_FILE) exists." || { echo "File $(SQLITE_FILE) does not exist." >&2; exit 1; }
	@if [ -z $(DATASETTE) ]; then echo "Datasette could not be found. See https://docs.datasette.io/en/stable/installation.html"; exit 2; fi
	@$(DATASETTE) -i $(SQLITE_FILE) --setting allow_download off --setting allow_csv_stream off --setting max_csv_mb 1 --setting default_cache_ttl 86400 --setting sql_time_limit_ms 2000 --metadata data/metadata.json --root

##@ Docker
.PHONY: docker-build
docker-build:   ## build datasette docker image.
	@[ -f $(SQLITE_FILE) ] && echo "File $(SQLITE_FILE) exists." || { echo "File $(SQLITE_FILE) does not exist." >&2; exit 1; }
	@if [ -z $(DOCKER) ]; then echo "Docker could not be found. See https://docs.docker.com/get-docker/"; exit 2; fi
	@if [ -z $(DATASETTE) ]; then echo "Datasette could not be found. See https://docs.datasette.io/en/stable/installation.html"; exit 2; fi
	$(DATASETTE) package $(SQLITE_FILE) --extra-options '--setting allow_download off --setting allow_csv_stream off --setting max_csv_mb 1 --setting default_cache_ttl 86400 --setting sql_time_limit_ms 2000' --metadata data/metadata.json --install=datasette-block-robots --install=datasette-vega --install=datasette-gzip --tag $(IMAGE_NAME):$(TAG_DATE)
	$(DATASETTE) package $(SQLITE_FILE) --extra-options '--setting allow_download off --setting allow_csv_stream off --setting max_csv_mb 1 --setting default_cache_ttl 86400 --setting sql_time_limit_ms 2000' --metadata data/metadata.json --install=datasette-block-robots --install=datasette-vega --install=datasette-gzip --tag $(IMAGE_NAME):latest

.PHONY: docker-push
docker-push:    ## push docker images to registry.
	@if [ -z $(DOCKER) ]; then echo "Docker could not be found. See https://docs.docker.com/get-docker/"; exit 2; fi
	@$(DOCKER) push $(IMAGE_NAME):$(TAG_DATE)
	@$(DOCKER) push $(IMAGE_NAME):latest
