`input-kafka-text` is an example showing how Primus adopts Kafka topics as training data source.
Similar to the examples with HDFS data, Primus reads data off from Kafka topics and injects them
into trainers.

## Preparation

The preparation of this example requires three actions

- setup a Primus cluster, refer to [quickstart](../../docs/primus-quickstart.md).
- modify `examples/input-kafka-text/primus_config` by updating `<kafka-broker-ip:port>`
- upload training data to Kafka
  ```bash
  # Create a new topic
  $ <kafka-bin>/kafka-topics.sh --bootstrap-server <kafka-broker> --create --topic primus-example-input-kafka-text
  # Upload data
  $ cat examples/input-kafka-text/data/* | <kafka-bin>/kafka-console-producer.sh --bootstrap-server <kafka-broker> --topic primus-example-input-kafka-text
  ```

## Execution

This example can be submitted with this following command, after which a Primus application will
start working and the execution can be verified by checking the logs or Primus UI as shown
in [quickstart](../../docs/primus-quickstart.md).

> Notes: This example cannot be rerun without cleaning Kafka offsets.

```bash
# Submit
$ primus-submit --primus_conf examples/input-file-text-timerange/primus_config.json

# Check the logs
$ <retrieve-logs> | grep Hello
Hello from input-kafka-text: 0-0
Hello from input-kafka-text: 0-1
Hello from input-kafka-text: 0-2
Hello from input-kafka-text: 0-3
Hello from input-kafka-text: 0-4
Hello from input-kafka-text: 0-5
Hello from input-kafka-text: 0-6
Hello from input-kafka-text: 0-7
...
Hello from input-kafka-text: 3-7
```