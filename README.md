[![Python Version](https://img.shields.io/badge/python-3.8-blue.svg)](https://github.com/getindata/streaming_jupyter_integrations)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/streaming_jupyter_integrations.svg)](https://pypi.org/project/streaming_jupyter_integrations/)
[![Downloads](https://pepy.tech/badge/streaming_jupyter_integrations)](https://pepy.tech/badge/streaming_jupyter_integrations)

# Streaming Jupyter Integrations

Streaming Jupyter Integrations project includes a set of magics for interactively running `Flink SQL`  jobs in [Jupyter](https://jupyter.org/) Notebooks

In order to actually use these magics, you must install our PIP package:

```shell
python3 -m pip install streaming-jupyter-integrations --extra-index-url https://__token__:<your_personal_token>@gitlab.com/api/v4/projects/32827938/packages/pypi/simple
```

In local development, when streaming-jupyter-integrations repository is checkouted on local machine, you may install the most current version:

```shell
python3 -m pip install -e .[tests]
```

The last thing to do is to register in Jupyter with a running IPython in the first cell:

```python
%load_ext streaming_jupyter_integrations.magics
```