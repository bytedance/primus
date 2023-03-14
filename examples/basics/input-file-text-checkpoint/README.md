`input-file-text-checkpoint` is an example demonstrating how Primus injects data into trainer
processes.
In this example, the Primus application is composed of a single role which reads from its stdin
which is
populated by Primus during execution with the data under an HDFS directory.

## Preparation

The preparation of this example only requires two actions 
- setup a Primus cluster, refer to [quickstart](../../docs/primus-quickstart.md).
- upload training data to HDFS
  ```
  $ hdfs dfs -mkdir -p /primus/examples/input-file-text-checkpoint 
  $ hdfs dfs -put examples/basics/input-file-text-checkpoint/data /primus/examples/input-file-text-checkpoint/
  ```

## Execution

This example can be submitted with this following command, after which a Primus application will
start working and the execution can be verified by checking the logs or Primus UI as shown
in [quickstart](../../docs/primus-quickstart.md).

```bash
# Submit
$ primus-submit --primus_conf examples/basics/input-file-text-checkpoint/primus_config.json

# Check the logs
$ <retrieve-logs> | grep Hello
0       Hello from input-file-text-checkpoint: 0-0
32      Hello from input-file-text-checkpoint: 0-1
64      Hello from input-file-text-checkpoint: 0-2
96      Hello from input-file-text-checkpoint: 0-3
128     Hello from input-file-text-checkpoint: 0-4
160     Hello from input-file-text-checkpoint: 0-5
192     Hello from input-file-text-checkpoint: 0-6
224     Hello from input-file-text-checkpoint: 0-7
...
224     Hello from input-file-text-checkpoint: 3-7
```

Once the Primus application completes, checkpoints can be found on HDFS at the path specified in the
Primus Configuration.
