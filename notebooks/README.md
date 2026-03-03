# Development Notebooks

Note: These notebooks are for manual testing only and are not part of production or CI workflows.

Use the project `piddi` environment and update it with the notebook dependencies, then start Jupyter.

## Update Environment

```bash
# From repo root
conda env update -n piddi -f notebooks/environment.yml
conda activate piddi
```

## Start Jupyter

```bash
jupyter lab
```