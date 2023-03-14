`hello-extended` is an example designed for demonstrating Primus UI which comprises Primus AM
UI for running Primus applications and Primus history UI for completed applications, and thus this
example has longer execution time compared to `hello` to ensure the time window to interact with
Primus AM UI, while Primus History UI can be accessed after the Primus application is completed. 

## Preparation

The preparation of this example only requires a Primus cluster, which setup can refer to
[quickstart](../../docs/primus-quickstart.md).


## Execution

This example can be submitted with this following command, after which a Primus application will
start working and the execution can be verified by checking the logs or Primus UI as shown
in [quickstart](../../docs/primus-quickstart.md).

```bash
# Submit
$ primus-submit --primus_conf examples/basics/hello-extended/primus_config.json

# Check the logs
$ <retrieve-logs> | grep Hello
Hello from Worker
Hello from Chief
Hello from PS
```