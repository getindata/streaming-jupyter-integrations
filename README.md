[![Python Version](https://img.shields.io/badge/python-3.8-blue.svg)](https://github.com/getindata/streaming_jupyter_integrations)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/streaming-jupyter-integrations.svg)](https://pypi.org/project/streaming-jupyter-integrations/)
[![Downloads](https://pepy.tech/badge/streaming_jupyter_integrations)](https://pepy.tech/badge/streaming_jupyter_integrations)

# Streaming Jupyter Integrations

Streaming Jupyter Integrations project includes a set of magics for interactively running `Flink SQL`  jobs in [Jupyter](https://jupyter.org/) Notebooks

In order to actually use these magics, you must install our PIP package:

```shell
python3 -m pip install streaming-jupyter-integrations
```

And then register in Jupyter with a running IPython in the first cell:

```python
%load_ext streaming_jupyter_integrations.magics
```

## Local development

In local development, when streaming-jupyter-integrations repository is checkouted on local machine, you may install the most current version:

```shell
python3 -m pip install -e .[tests]
```

The project uses [pre-commit](https://pre-commit.com/) hooks to ensure code quality, mostly by linting. To use it, [install pre-commit](https://pre-commit.com/#install) and then run
```shell
pre-commit install --install-hooks
```
From that moment, it will lint the files you have modified on every commit attempt.
