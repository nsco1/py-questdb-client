ifeq (, $(shell which docker))
$(error "Command docker not found in: $(PATH)")
endif

ifeq (, $(shell which curl))
$(error "Command curl not found in: $(PATH)")
endif

poetry ?= poetry
docker ?= docker
make   ?= make

test:
	$(poetry) run pytest tests

install-dependencies:
	$(poetry) install

format-code:
	$(poetry) run black questdb_ilp_client tests
	$(poetry) run isort questdb_ilp_client tests --profile black

check-code-quality:
	$(poetry) run flake8 questdb_ilp_client tests
	$(poetry) run black questdb_ilp_client tests --check
	$(poetry) run isort questdb_ilp_client tests --check-only --profile black
	$(poetry) run bandit -r questdb_ilp_client tests
	$(poetry) run safety check

compose-up:
	docker-compose -f docker-compose.yaml up -d

compose-down:
	docker-compose -f docker-compose.yaml down --remove-orphans
	echo "y" | $(docker) container prune
	echo "y" | $(docker) volume prune
