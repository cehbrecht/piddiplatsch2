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

test: ## run tests quickly with the default Python (skip slow and online tests)
	@echo "Running tests (skip slow and online tests) ..."
	@bash -c 'pytest -v -m "not slow and not online" tests/'

test-smoke: ## run smoke tests only and in parallel
	@echo "Running smoke tests (only online tests) ..."
	@bash -c 'pytest -v -m "online" tests/'

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

## Docker container targets

start: ## builds and starts docker containers
	docker-compose up --build -d

stop: ## stops docker containers
	docker-compose down -v