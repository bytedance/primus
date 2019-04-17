# Primus

Primus is a generic distributed scheduling framework for machine learning applications, which
manages training lifecycle and data distribution for machine learning trainers such as
TensorFlow to perform distributed training in massive scales. For more information, please visit [overview](docs/primus-overview.md)
and [quickstart](docs/primus-quickstart.md).

## Building Primus

Primus is built using [Apache Maven](https://maven.apache.org/).
To build Spark and its example programs, run:

```bash
mvn -DskipTests clean package
```

For general deployments, environment configurations are required, see the setups
in [quickstart](../docs/primus-quickstart.md) for references.

## Contribution

Primus is under active development, and we use GitHub issues for tracking requests and bugs, feel
free to contact us for any assistance. If you want to contribute to Primus, you are expected to
uphold our [code of conduct](docs/primus-code-of-conduct.md).

## Security

If any potential security issue is discovered, please **do not** create a public GitHub issue.
Instead, **do** inform Bytedance Security directly via
our [security center](https://security.bytedance.com/src)
or [vulnerability reporting email](mailto:sec@bytedance.com).

## License

[Apache License 2.0](LICENSE)
