# Streaming Jupyter Integrations

Streaming Jupyter Integrations project includes a set of magics for interactively running `Flink SQL`  jobs in [Jupyter](https://jupyter.org/) Notebooks

In order to actually use these magics, you must install our PIP package:

```shell
python3 -m pip install streaming-jupyter-integrations --extra-index-url https://__token__:<your_personal_token>@gitlab.com/api/v4/projects/29597698/packages/pypi/simple
```

In local development, when streaming-jupyter-integrations repository is checkouted on local machine, you may install the most current version:

```shell
python3 -m pip install -e .[tests]
```

The last thing to do is to register in Jupyter with a running IPython in the first cell:

```python
%load_ext streaming_jupyter_integrations.magics
```