# RTX-KG2 Gateway

Enabling RTX-KG2 data access through various means.

## Installation

Please use Python [`poetry`](https://python-poetry.org/) to run and install related content.
The Poetry environment for this project includes dependencies which help run IDE environments, manage the data, and run workflows.

```bash
# after installing poetry, create the environment
poetry install
```

## Development

### Jupyter Lab

Please follow installation steps above and then use a relevant Jupyter environment to open and explore the notebooks under the `notebooks` directory.

```bash
# after creating poetry environment, run jupyter
poetry run jupyter lab
```

### Poe the Poet

We use [Poe the Poet](https://poethepoet.natn.io/index.html) to define and run tasks defined within `pyproject.toml` under the section `[tool.poe.tasks*]`.
This allows for the definition and use of a task workflow when implementing multiple procedures in sequence.

For example, use the following to run the `notebook_sample_data_generation` task:

```bash
# run data_prep task using poethepoet defined within `pyproject.toml`
poetry run poe notebook_sample_data_generation
```

Existing tasks:

- `notebook_sample_data_generation`: generates a sample parquet dataset and adds to a kuzu database.
- `notebook_full_data_generation`: generates full dataset and adds to a kuzu database.
