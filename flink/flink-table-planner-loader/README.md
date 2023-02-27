# flink-table-planner-loader

The goal of this project is to provide a fix for [FLINK-23020](https://issues.apache.org/jira/browse/FLINK-23020)
without maintaining custom flink fork. The fix is needed
for [streaming-jupyter-integrations](https://github.com/getindata/streaming-jupyter-integrations) to run correctly.

The idea is pretty simple. `FlinkRelMetadataQuery` is compiled with
the [fix](https://github.com/apache/flink/pull/20655) applied. Then the compiled `.class` files are added to the
official `flink-table-planner-loader` jar. Then the modified jar should be added to `pyflink/lib` directory on a master
node where Jupyter Notebooks run.


```bash
FLINK_VERSION=1.15.1 ./modify-jar.sh
```
