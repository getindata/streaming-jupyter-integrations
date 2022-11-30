[![Python Version](https://img.shields.io/badge/python-3.8-blue.svg)](https://github.com/getindata/streaming_jupyter_integrations)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/streaming-jupyter-integrations.svg)](https://pypi.org/project/streaming-jupyter-integrations/)
[![Downloads](https://pepy.tech/badge/streaming_jupyter_integrations)](https://pepy.tech/badge/streaming_jupyter_integrations)

# Streaming Jupyter Integrations

Streaming Jupyter Integrations project includes a set of magics for interactively running _Flink SQL_  jobs in [Jupyter](https://jupyter.org/) Notebooks

## Installation

In order to actually use these magics, you must install our PIP package along `jupyterlab-lsp`:

```shell
python3 -m pip install jupyterlab-lsp streaming-jupyter-integrations
```

## Usage

Register in Jupyter with a running IPython in the first cell:

```python
%load_ext streaming_jupyter_integrations.magics
```

Then you need to decide which _execution mode_ and _execution target_ to choose.

```python
%flink_connect --execution-mode [mode] --execution-target [target]
```

By default, the `streaming` execution mode and `local` execution target are used.

```python
%flink_connect
```

### Execution mode

Currently, Flink supports two execution modes: _batch_ and _streaming_. Please see
[Flink documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/execution_mode/)
for more details.

In order to specify execution mode, add `--execution-mode` parameter, for instance:
```python
%flink_connect --execution-mode batch
```

### Execution target

Streaming Jupyter Integrations supports 3 execution targets:
* Local
* Remote
* YARN Session

#### Local execution target

Running Flink in `local` mode will start a MiniCluster in a local JVM with parallelism 1.

In order to run Flink locally, use:
```python
%flink_connect --execution-target local
```

Alternatively, since the execution target is `local` by default, use:
```python
%flink_connect
```

One can specify port of the local JobManager (8099 by default). This is useful especially if you run multiple
Notebooks in a single JupyterLab.

```python
%flink_connect --execution-target local --local-port 8123
```


#### Remote execution target

Running Flink in remote mode will connect to an existing Flink session cluster. Besides specifying `--execution-target`
to be `remote`, you also need to specify `--remote-hostname` and `--remote-port` pointing to Flink Job Manager's
REST API address.

```python
%flink_connect \
    --execution-target remote \
    --remote-hostname example.com \
    --remote-port 8888
```

#### YARN session execution target

Running Flink in `yarn-session` mode will connect to an existing Flink session cluster running on YARN. You may specify
the hostname and port of the YARN Resource Manager (`--resource-manager-hostname` and `--resource-manager-port`).
If Resource Manager address is not provided, it is assumed that notebook runs on the same node as Resource Manager.
You can also specify YARN applicationId (`--yarn-application-id`) to which the notebook will connect to.
If `--yarn-application-id` is not specified and there is one YARN application running on the cluster, the notebook will
try to connect to it. Otherwise, it will fail.

Connecting to a remote Flink session cluster running on a remote YARN cluster:
```python
%flink_connect \
    --execution-target yarn-session \
    --resource-manager-hostname example.com \
    --resource-manager-port 8888 \
    --yarn-application-id application_1666172784500_0001
```

Connecting to a Flink session cluster running on a YARN cluster:
```python
%flink_connect \
    --execution-target yarn-session \
    --yarn-application-id application_1666172784500_0001
```

Connecting to a Flink session cluster running on a dedicated YARN cluster:
```python
%flink_connect --execution-target yarn-session
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

### `%%flink_execute` command

The command allows to use Python DataStream API and Table API. There are two handles exposed for each API:
`stream_env` and `table_env`, respectively.

Table API example:
```python
%%flink_execute
query = """
    SELECT   user_id, COUNT(*)
    FROM     orders
    GROUP BY user_id
"""
execution_output = table_env.execute_sql(query)
```

When Table API is used, the final result has to be assigned to `execution_output` variable.

DataStream API example:
```python
%%flink_execute
from pyflink.common.typeinfo import Types

execution_output = stream_env.from_collection(
    collection=[(1, 'aaa'), (2, 'bb'), (3, 'cccc')],
    type_info=Types.ROW([Types.INT(), Types.STRING()])
)
```

When DataStream API is used, the final result has to be assigned to `execution_output` variable. Please note that
the pipeline does not end with `.execute()`, the execution is triggered by the Jupyter magics under the hood.

---

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
