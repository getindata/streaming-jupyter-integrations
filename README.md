[![Python Version](https://img.shields.io/badge/python-3.8-blue.svg)](https://github.com/getindata/streaming_jupyter_integrations)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/streaming-jupyter-integrations.svg)](https://pypi.org/project/streaming-jupyter-integrations/)
[![Downloads](https://pepy.tech/badge/streaming_jupyter_integrations)](https://pepy.tech/badge/streaming_jupyter_integrations)

# Streaming Jupyter Integrations

Streaming Jupyter Integrations project includes a set of magics for interactively running _Flink SQL_  jobs in [Jupyter](https://jupyter.org/) Notebooks

In order to actually use these magics, you must install our PIP package along `jupyterlab-lsp`:

```shell
python3 -m pip install jupyterlab-lsp streaming-jupyter-integrations
```

And then register in Jupyter with a running IPython in the first cell:

```python
%load_ext streaming_jupyter_integrations.magics
```

## Variables
Magics allow for dynamic variable substitution in _Flink SQL_ cells.
```python
my_variable = 1
```
```sql
SELECT * FROM some_table WHERE product_id = {my_variable}
```

Moreover, you can mark sensitive variables like password so they will be read from environment variables or user input every time one runs the cell:
```sql
CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
   'table-name' = 'users',
   'username' = '${my_username}',
   'password' = '${my_password}'
);
```

## Local development

Note: You will need NodeJS to build the extension package.

The `jlpm` command is JupyterLab's pinned version of
[yarn](https://yarnpkg.com/) that is installed with JupyterLab. You may use
`yarn` or `npm` in lieu of `jlpm` below. In order to use `jlpm`, you have to
have `jupyterlab` installed (e.g., by `brew install jupyterlab`, if you use
Homebrew as your package manager).

```bash
# Clone the repo to your local environment
# Change directory to the flink_sql_lsp_extension directory
# Install package in development mode
pip install -e .
# Link your development version of the extension with JupyterLab
jupyter labextension develop . --overwrite
# Rebuild extension Typescript source after making changes
jlpm build
```

The project uses [pre-commit](https://pre-commit.com/) hooks to ensure code quality, mostly by linting.
To use it, [install pre-commit](https://pre-commit.com/#install) and then run
```shell
pre-commit install --install-hooks
```
From that moment, it will lint the files you have modified on every commit attempt.
