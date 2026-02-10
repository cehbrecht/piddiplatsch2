# Configuration
APP_ROOT := $(abspath $(lastword $(MAKEFILE_LIST))/..)
APP_NAME := piddiplatsch

# end of configuration

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
	else:
		match = re.match(r'^## (.*)$$', line)
		if match:
			help = match.groups()[0]
			print("\n%s" % (help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

.DEFAULT_GOAL := help

help: ## print this help message. (Default)
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

## Build targets:

install: ## install application
	@echo "Installing application ..."
	@-bash -c 'pip install -e .'
	@echo "\nStart service with \`make start\` and stop with \`make stop\`."

develop: ## install application with development libraries
	@echo "Installing development requirements for tests and docs ..."
	@-bash -c 'pip install -e ".[dev]"'

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	@echo "Removing build artifacts ..."
	@-rm -fr build/
	@-rm -fr dist/
	@-rm -fr .eggs/
	@-find . -name '*.egg-info' -exec rm -fr {} +
	@-find . -name '*.egg' -exec rm -f {} +
	@-find . -name '*.log' -exec rm -fr {} +
	@-find . -name '*.sqlite' -exec rm -fr {} +
	@-find . -name '*.db' -exec rm -fr {} +

clean-pyc: ## remove Python file artifacts
	@echo "Removing Python file artifacts ..."
	@-find . -name '*.pyc' -exec rm -f {} +
	@-find . -name '*.pyo' -exec rm -f {} +
	@-find . -name '*~' -exec rm -f {} +
	@-find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	@echo "Removing test artifacts ..."
	@-rm -fr .tox/
	@-rm -f .coverage
	@-rm -fr .pytest_cache

clean-dist: clean  ## remove git ignored files and directories
	@echo "Running 'git clean' ..."
	@git diff --quiet HEAD || echo "There are uncommitted changes! Aborting 'git clean' ..."
	## do not use git clean -e/--exclude here, add them to .gitignore instead
	@-git clean -dfx

clean-docs: ## remove documentation artifacts
	@echo "Removing documentation artifacts ..."
	$(MAKE) -C docs clean

lint: ## check style with ruff, black, isort
	@echo "Running code style checks (ruff, black, isort) ..."
	@bash -c 'ruff check src tests'
	@bash -c 'black --check src tests'
	@bash -c 'isort --check-only src tests'

fix: ## fix linting errors automatically
	@echo "Running ruff with fix option ..."
	@bash -c 'ruff check --fix src tests'

format: ## format code using isort and black
	@echo "Formatting code with isort and black ..."
	@bash -c 'isort src tests'
	@bash -c 'black src tests'

check-format: ## check that code is correctly formatted
	@echo "Checking code formatting with isort and black ..."
	@bash -c 'isort --check-only src tests'
	@bash -c 'black --check src tests'

pre-commit: ## run all pre-commit hooks
	@pre-commit run --all-files


## Testing targets:

test: test-unit test-integration ## run all fast tests (unit + integration, no Docker required)

test-unit: ## run unit tests only (fast, no external dependencies - unmarked tests)
	@echo "Running unit tests ..."
	@bash -c 'pytest -v -m "not integration and not smoke" tests/'

test-integration: ## run integration tests only (JSONL backend, no Docker required)
	@echo "Running integration tests ..."
	@bash -c 'pytest -v -m "integration" tests/'

test-smoke: start-docker ## run smoke tests only (requires Docker: Kafka + Handle server)
	@echo "Running smoke tests ..."
	# Ensure Kafka topic exists before starting consumer
	@echo "Ensuring Kafka topic exists (from config) ..."
	@bash scripts/ensure_kafka_topic.sh || true
	# Start production consumer in background
	@echo "Starting piddi consumer (background) ..."
	@bash -c 'piddi --config tests/config.toml consume & echo $$! > .consumer.pid && echo "Consumer PID: $$(cat .consumer.pid)"'
	# Run smoke tests; on failure, ensure consumer and docker are stopped
	@bash -c 'pytest -v -s -m "smoke" tests/' || ($(MAKE) stop-consumer; $(MAKE) stop-docker; exit 1)
	# Stop consumer and docker after tests
	@$(MAKE) stop-consumer
	@$(MAKE) stop-docker

test-all: test-unit test-integration test-smoke ## run all tests including smoke tests

smoke: test-smoke

coverage: ## check code coverage quickly with the default Python
	@bash -c 'coverage run --source piddiplatsch -m pytest'
	@bash -c 'coverage report -m'
	@bash -c 'coverage html'
	$(BROWSER) htmlcov/index.html

## Deployment targets:

dist: clean ## builds source and wheel package
	python -m flit build
	ls -l dist

release: dist ## package and upload a release
	python -m flit publish dist/*

upstream: develop ## install the GitHub-based development branches of dependencies in editable mode to the active Python's site-packages
	python -m pip install --no-user --requirement requirements_upstream.txt

## Versioning targets

bump-patch: ## bump patch version and create git tag
	@echo "Bumping patch version..."
	@bump-my-version bump patch

bump-minor: ## bump minor version and create git tag
	@echo "Bumping minor version..."
	@bump-my-version bump minor

bump-major: ## bump major version and create git tag
	@echo "Bumping major version..."
	@bump-my-version bump major

## Docker test services targets

start-docker: ## start Docker services (Kafka + Handle server) for testing
	@echo "======================================================================"
	@echo "🐳 Starting Docker services (Kafka + Handle server)..."
	@echo "======================================================================"
	@docker-compose up --build -d
	@echo "🔍 Checking Kafka readiness on localhost:39092 (max 25s)..."
	@bash -c 'retries=25; i=0; while [ $$i -lt $$retries ]; do if nc -z localhost 39092 >/dev/null 2>&1; then echo "✅ Kafka is ready!"; exit 0; fi; sleep 1; i=$$((i+1)); done; echo "⚠️ Kafka may not be ready yet; proceeding anyway."'
	@echo "✅ Docker services started!"
	@echo ""

stop-docker: ## stop Docker test services
	@echo "======================================================================"
	@echo "🐳 Stopping Docker services..."
	@echo "======================================================================"
	@docker-compose down -v
	@echo "✅ Docker services stopped!"
	@echo ""

# Local consumer management
start-consumer:
	@echo "Starting piddi consumer ..."
	# Ensure Kafka topic exists before starting consumer
	@echo "Ensuring Kafka topic exists (from config) ..."
	@bash -c 'python -c "from piddiplatsch.config import config; config.load_user_config(\"tests/config.toml\"); from piddiplatsch.testing.kafka_client import ensure_topic_exists_from_config; ensure_topic_exists_from_config()"'
	@bash -c 'piddi --config tests/config.toml consume & echo $$! > .consumer.pid && echo "Consumer PID: $$(cat .consumer.pid)"'

stop-consumer:
	@echo "Stopping piddi consumer ..."
	@bash -c 'if [ -f .consumer.pid ]; then kill $$(cat .consumer.pid) >/dev/null 2>&1 || true; rm -f .consumer.pid; else echo "No consumer PID file"; fi'

docker-build: ## build Docker images for test services
	@echo "Building Docker images..."
	@docker-compose build

docker-clean: stop-docker ## remove all Docker images and volumes
	@echo "======================================================================"
	@echo "🧹 Cleaning Docker images..."
	@echo "======================================================================"
	@docker image prune -f
	@echo "✅ Docker cleaned!"
	@echo ""