---

## 🔌 Plugins

- One plugin active at a time, selected via `consumer.processor` (e.g., "cmip6").
- Config for each plugin lives under `[plugins.<name>]` in the TOML.
- Example (CMIP6):

```toml
[plugins.cmip6]
landing_page_url = "https://handle-esgf.dkrz.de/lp"
max_parts = -1
excluded_asset_keys = ["reference_file", "globus", "thumbnail", "quicklook"]
```

To add a plugin:
- Implement a processor under `src/piddiplatsch/plugins/<name>/processor.py`.
- Register it in the static registry (see `piddiplatsch.core.registry.register_processor("<name>", YourProcessor)`).
- Provide `[plugins.<name>]` config as needed.

# Contributing to Piddiplatsch

Thank you for your interest in contributing to Piddiplatsch! This guide will help you get started with development, testing, and making contributions.

---

## 🚀 Development Setup

### Prerequisites

- [Miniconda (via conda-forge)](https://conda-forge.org/download/)
- Git

### Setting Up Your Development Environment

```bash
# Clone the repository
git clone git@github.com:cehbrecht/piddiplatsch2.git
cd piddiplatsch2

# Create and activate conda environment
conda env create
conda activate piddiplatsch2

# Install in development mode with dev dependencies
pip install -e ".[dev]"
# OR
make develop
```

---

## ✅ Testing

This project uses a three-tier testing strategy to ensure code quality at different levels.

### Test Types

**Unit Tests** (fast, no external dependencies)
- Located in `tests/`
- Pure logic tests with mocked dependencies
- No pytest markers required
- Run on every commit

**Integration Tests** (medium speed, JSONL backend)
- Located in `tests/integration/`
- Tests component interactions using JSONL backend
- Marked with `@pytest.mark.integration`
- No Docker required
- Uses test configuration from `tests/config.toml`

**Smoke Tests** (end-to-end, requires Docker)
- Located in `tests/smoke/`
- Full workflow tests with Kafka + mock Handle server
- Marked with `@pytest.mark.smoke`
- Docker services started/stopped automatically
- **Note:** Docker is only for testing, not production

### Running Tests

Run all fast tests (unit + integration):

```bash
make test
```

Run only unit tests:

```bash
make test-unit
```

Run only integration tests:

```bash
make test-integration
```

Run smoke tests (Docker services started automatically):

```bash
make smoke
# or
make test-smoke
```

Run all tests including smoke tests:

```bash
make test-all
```

### Docker Management for Testing

Docker is only used for smoke tests. The containers provide:
- Kafka cluster (3 nodes)
- Mock Handle server

Kafka readiness can be slightly delayed; the workflow uses a simple port check (`nc -z`) and a Python helper to ensure the topic exists with retries.

Start Docker services manually:

```bash
make start-docker
```

Stop Docker services:

```bash
make stop-docker
```

Rebuild Docker images:

```bash
make docker-build
```

Clean up Docker images and volumes:

```bash
make docker-clean
```

---

## 🔧 CLI Usage & Options

Requires external Kafka and Handle services (or use the local Docker stack for smoke).

- Start consumer: `piddi consume`
- Common flags:
   - `--config <path>`: point to your TOML config
   - `--verbose`: more logging
   - `--debug --log my.log`: enable debug and log to file
   - `--dump`: write incoming messages to `outputs/dump/`
   - `--dry-run`: write handle records to JSONL without contacting Handle Service
   - `--force`: continue on transient external failures (e.g., STAC outages)

### Observe mode (example)

For exploratory runs without real Handle writes, use the relaxed example config together with dry-run:

```bash
cp etc/observe.toml .
piddi --config observe.toml consume --dry-run --dump --force
```

This configuration:
- Sets `consumer.max_errors=1000` and `stop_on_skip=false` to keep processing
- Uses `handle.backend=jsonl` for local record output
- Disables strict schema checks (`schema.strict_mode=false`)

When ready for production, run with your normal config (defaults are conservative) and omit `--dry-run` and `--force`.

## 🧼 Code Style and Linting

This project uses automated tools to maintain code quality:

- [`black`](https://black.readthedocs.io) - Code formatting
- [`isort`](https://pycqa.github.io/isort/) - Import sorting
- [`ruff`](https://docs.astral.sh/ruff/) - Linting and fast checks
- [`pre-commit`](https://pre-commit.com) - Git hooks for automated checks

### Setup Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
```

This will automatically run checks before each commit.

### Manual Checks

Run all pre-commit checks:

```bash
pre-commit run --all-files
```

Or use Makefile targets:

```bash
make lint         # Run ruff, black, and isort checks
make format       # Auto-format with black and isort
make check-format # Check formatting only
make fix          # Fix linting errors automatically
```

---

## 📦 Versioning and Releases

This project uses [bump-my-version](https://github.com/callowayproject/bump-my-version) for version management.

### Bumping Version

```bash
# Preview version bump (dry-run)
bump-my-version bump patch --dry-run --allow-dirty

# Bump version and create git tag
make bump-patch    # 1.0.0 → 1.0.1
make bump-minor    # 1.0.0 → 1.1.0
make bump-major    # 1.0.0 → 2.0.0

# Push changes and tags
git push && git push --tags
```

The version is stored in `pyproject.toml` and automatically tagged in git.

---

## 🔄 Development Workflow

1. **Create a branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Write code
   - Add/update tests
   - Update documentation if needed

3. **Run tests and linting**
   ```bash
   make lint
   make test
   ```

4. **Commit your changes**
   ```bash
   git add .
   git commit -m "Description of your changes"
   ```
   Pre-commit hooks will run automatically.

5. **Push and create Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```
   Then create a PR on GitHub.

---

## 📝 Writing Tests

### Unit Tests

Place unit tests in `tests/` with descriptive names:

```python
# tests/test_my_module.py
def test_function_does_something():
    result = my_function(input_data)
    assert result == expected_output
```

### Integration Tests

Place integration tests in `tests/integration/` and mark them:

```python
# tests/integration/test_my_integration.py
import pytest

pytestmark = pytest.mark.integration

def test_pipeline_with_jsonl_backend():
    # Test component interactions
    pass
```

### Smoke Tests

Place smoke tests in `tests/smoke/` and mark them:

```python
# tests/smoke/test_end_to_end.py
import pytest

pytestmark = pytest.mark.smoke

def test_full_workflow(handle_client, testfile):
    # Test complete workflow
    pass
```

---

## 🐛 Reporting Issues

When reporting issues, please include:
- Python version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

---

## 💡 Questions?

Feel free to:
- Open an issue for questions
- Start a discussion on GitHub
- Check existing issues and PRs

---

Thank you for contributing to Piddiplatsch! 🎉
