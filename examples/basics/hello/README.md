`hello` is an example demonstrating how Primus manages training lifecycles. In this example, three
the Primus application is composed of three different training roles, and configured to complete
when all three of them finish their works, which are simply saying hello on stdout.

## Preparation

The preparation of this example only requires a Primus cluster, which setup can refer to
[quickstart](../../docs/primus-quickstart.md).


## Execution

This example can be submitted with this following command, after which a Primus application will
start working and the execution can be verified by checking the logs or Primus UI as shown
in [quickstart](../../docs/primus-quickstart.md).

```bash
# Submit
$ primus-submit --primus_conf examples/basics/hello/primus_config.json

# Check the logs
$ <retrieve-logs> | grep Hello
Hello from Worker
Hello from Chief
Hello from PS
```