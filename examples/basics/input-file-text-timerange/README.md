`input-file-text-timerange` is an example demonstrating an advanced feature of Primus data
injection. In addition to reading from a single HDFS directory, Primus is equipped with the
capability of scanning subdirectory with time range. That is, by specifying the desired time range
and the corresponding subdirectory naming format, Primus will inject all the data under the
conforming directories.

## Preparation

The preparation of this example only requires two actions
- setup a Primus cluster, refer to [quickstart](../../docs/primus-quickstart.md).
- upload training data to HDFS
  ```
  $ hdfs dfs -mkdir -p /primus/examples/input-file-text-timerange 
  $ hdfs dfs -put examples/input-file-text/data /primus/examples/input-file-text-timerange/
  ```

## Execution

This example can be submitted with this following command, after which a Primus application will
start working and the execution can be verified by checking the logs or Primus UI as shown
in [quickstart](../../docs/primus-quickstart.md).

```bash
# Submit
$ primus-submit --primus_conf examples/input-file-text-timerange/primus_config.json

# Check the logs
$ <retrieve-logs> | grep Hello
0       Hello from input-file-text-timerange: 2020-01-01 - 0
53      Hello from input-file-text-timerange: 2020-01-01 - 1
106     Hello from input-file-text-timerange: 2020-01-01 - 2
159     Hello from input-file-text-timerange: 2020-01-01 - 3
...
159     Hello from input-file-text-timerange: 2020-01-04 - 3
```