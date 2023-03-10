`input-file-customized` is an example demonstrating how CustomizedFileDataSource works.

## Preparation

The preparation of this example only requires two actions 
- setup a Primus cluster, refer to [quickstart](../../docs/primus-quickstart.md).
- upload training data to HDFS
  ```
  $ hdfs dfs -mkdir -p /primus/examples/input-file-customized 
  $ hdfs dfs -put examples/basics/input-file-customized/data /primus/examples/input-file-customized/
  ```

## Execution

This example can be submitted with this following command, after which a Primus application will
start working and the execution can be verified by checking the logs or Primus UI as shown
in [quickstart](../../docs/primus-quickstart.md).

```bash
# Submit
$ primus-submit --primus_conf examples/basics/input-file-customized/primus_config.json

# Check the logs
$ <retrieve-logs> | grep Hello
KP-0-KS	VP-Hello from input-file-customized: 0-0-VS
KP-38-KS	VP-Hello from input-file-customized: 0-1-VS
KP-76-KS	VP-Hello from input-file-customized: 0-2-VS
KP-114-KS	VP-Hello from input-file-customized: 0-3-VS
...
KP-114-KS	VP-Hello from input-file-customized: 3-3-VS
```