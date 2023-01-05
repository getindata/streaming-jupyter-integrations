# Changelog

## [Unreleased]

## [0.12.1] - 2023-01-05

### Added

-   Try to load `flink-conf.yaml` from `FLINK_CONF_DIR` first.

### Fixed

-   Fix execution of SHOW, EXPLAIN and DESCRIBE commands.
-   Speed up results display.
-   Do not truncate results when SHOW, EXPLAIN or DESCRIBE command is executed.

## [0.12.0] - 2022-11-30

### Added

-   Support Java 8.
-   Parallelism argument for flink_execute_sql.
-   Add `%flink_execute` and `%flink_execute_file` command which allows to execute low-level Python Flink API.

### Fixed

-   Add a workaround for displaying `TIMESTAMP_LTZ` field.

## [0.11.0] - 2022-11-03

### Added

-   Add `row_kind` column to the results.
-   Allow specifying port in local execution target (`--local-port`).

### Fixed

-   In yarn-session execution target only RUNNING yarn applications are taken into account.

## [0.10.0] - 2022-10-25

### Added

-   Support for execution targets: local, remote, yarn-session.

### Changed

-   Accept Apache Flink versions 1.15.X.

## [0.9.1] - 2022-10-25

### Added

-   Show functions & change icon for views in `%flink_show_table_tree`

### Fixed

-   Skip curly brackets without variable

## [0.9.0] - 2022-10-20

### Added

-   Support for batch mode

### Fixed

-   Add backticks to escape table/database/catalog names.
-   Print error if query execution fails.
-   Fix TableResult#wait() timeout parameter.
-   Fix for batch queries with an empty result.

## [0.8.2] - 2022-10-07

### Changed

-   Update Apache Flink dependency to 1.15.2.

## [0.8.1] - 2022-10-07

### Fixed

-   Do not add comments to the inline query

## [0.8.0] - 2022-10-05

### Added

-   Enable users to display catalogs, databases, tables and columns as an expandable tree by running `%flink_show_table_tree` command.

## [0.7.0] - 2022-09-23

### Added

-   Support for python 3.7

## [0.6.2] - 2022-09-14

### Fixed

-   Do not throw if `secrets` is nonexistent in the [streaming CLI](https://github.com/getindata/streaming-cli/) config file.

## [0.6.1] - 2022-09-14

### Fixed

-   Ensure `_secrets` variable is set at the beginning of magics initialization.

## [0.6.0] - 2022-09-13

### Changed

-   Read variable marked with `${VARIABLE_NAME}` from environment variables, if it exists. Otherwise, ask user using `getpass` module.

### Fixed

-   Ensure that `jars` directory exists before plugins loading.

## [0.5.0] - 2022-09-12

### Added

-   Use `getpass` to read variables marked with `${VARIABLE_NAME}` from user input.
-   Load secret from file using `%load_secret_file` magic.
-   Load secrets from files listed in `.streaming_config.yml` file.

## [0.4.0] - 2022-08-25

### Fixed

-   Make Jupyter update DataFrame with the data polled by Flink in real time.

## [0.3.0] - 2022-08-22

### Added

-   Using init.sql as the initialization script.
-   Use plugin to run jar providers on initialization.

### Fixed

-   Make SQL syntax highlighting available in JupyterLab.
-   Make Magics recognize _DESCRIBE_ and _SHOW_ keywords as queries.

## [0.2.1] - 2022-08-02

### Changed

-   Update Apache Flink dependency to 1.15.1.

## [0.2.0] - 2022-07-06

### Changed

-   Update Apache Flink dependency to 1.15.

## [0.1.6] - 2022-06-17

### Added

-   StreamingEnvironment uses `flink-conf.yaml` properties.

## [0.1.5] - 2022-05-11

## [0.1.4] - 2022-05-10

## [0.1.3] - 2022-05-09

-   First release

[Unreleased]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.12.1...HEAD

[0.12.1]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.12.0...0.12.1

[0.12.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.11.0...0.12.0

[0.11.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.10.0...0.11.0

[0.10.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.9.1...0.10.0

[0.9.1]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.9.0...0.9.1

[0.9.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.8.2...0.9.0

[0.8.2]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.8.1...0.8.2

[0.8.1]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.8.0...0.8.1

[0.8.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.7.0...0.8.0

[0.7.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.6.2...0.7.0

[0.6.2]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.6.1...0.6.2

[0.6.1]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.6.0...0.6.1

[0.6.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.5.0...0.6.0

[0.5.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.4.0...0.5.0

[0.4.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.2.1...0.3.0

[0.2.1]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.2.0...0.2.1

[0.2.0]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.1.6...0.2.0

[0.1.6]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.1.5...0.1.6

[0.1.5]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.1.4...0.1.5

[0.1.4]: https://github.com/getindata/streaming-jupyter-integrations/compare/0.1.3...0.1.4

[0.1.3]: https://github.com/getindata/streaming-jupyter-integrations/compare/bfc6e43c26bfa549540e58eb24de25954540a24c...0.1.3
