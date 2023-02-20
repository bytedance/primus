In this directory, several examples are provided to demonstrate how Primus is integrated with
TensorFlow. Noticeably, dedicated configurations are required for different Primus runtimes, this
document is prepared for Primus on Kubernetes with [Primus Baseline VM](docs/primus-quickstart.md).

---

# Primus on Kubernetes

To support TensorFlow distributed training, one major responsibility of Primus is distributing both
the training resources such as scripts and the corresponding environment. As shown below, both of
them are supported by Primus configuration for each application, where the former only requires
listing the needed resources, while the latter can be straightforwardly achieved by specifying the
docker image for Primus executors.

```json
{
  ...
  "files": [
    "tensorflow-single"
  ],
  ...
  "runtimeConf": {
    "kubernetesNativeConf": {
      "executorPodConf": {
        "mainContainerConf": {
          "imageName": "primus-baseline-tensorflow:latest"
        }
      }
    }
  }
}
```

## Preparation

As shared previously, docker images are required for executing TensorFlow distributed training
applications with Primus, which can be built and loaded to Primus Baseline VM with the command shown
below.

```shell
$ cd /usr/lib/primus-kubernetes/examples/tensorflow/resources/containers/primus-baseline-tensorflow
$ docker build -t primus-baseline-tensorflow .
$ kind load docker-image primus-baseline-tensorflow
```

Then, upload [mnist database](http://yann.lecun.com/exdb/mnist/) to HDFS as training data.

```shell
$ cd /usr/lib/primus-kubernetes/examples/tensorflow
$ /usr/lib/hadoop/bin/hdfs dfs -mkdir -p /primus/examples
$ /usr/lib/hadoop/bin/hdfs dfs -put resources/data/mnist /primus/examples/
```

Lastly, install python packages required to evaluate the models locally, and we are good to go.

```shell
$ python3 -m pip install 'pandas==1.5.3'
$ python3 -m pip install 'tensorflow==2.8.0'
$ python3 -m pip install 'tensorflow-io==0.24.0'
$ python3 -m pip install 'protobuf==3.20.1'
```
