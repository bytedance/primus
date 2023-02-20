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

## TensorFlow Single

TensorFlow single is an example derived from the [TensorFlow official tutorial](
https://www.tensorflow.org/tutorials/quickstart/beginner) to demonstrate the workflow with Primus
which covers the basic TensorFlow usages including reading from HDFS, scheduling data, training
model and finally persisting the model onto HDFS.

**To execute**

```shell
$ cd /usr/lib/primus-kubernetes/examples/tensorflow
$ /usr/lib/primus-kubernetes/sbin/primus-submit --primus_conf tensorflow-single/primus_config.json
...
[2023-02-20 14:51:22:258] [INFO] - com.bytedance.primus.runtime.kubernetesnative.client.KubernetesSubmitCmdRunner.lambda$doWaitAppCompletion$0(KubernetesSubmitCmdRunner.java:151) current driver pod status: Succeeded
[2023-02-20 14:51:22:258] [INFO] - com.bytedance.primus.runtime.kubernetesnative.client.Client.execute(Client.java:56) Shutting down LogManager
...
```

**To evaluate**

```shell
$ cd /usr/lib/primus-kubernetes/examples/tensorflow
$ bash resources/data/mnist/evaluate.sh resources/data/mnist/test hdfs:///primus/examples/mnist/models/tensorflow-single
...
313/313 - 0s - loss: 1.9948 - accuracy: 0.8274 - 394ms/epoch - 1ms/step
Model accuracy: [1.9948195219039917, 0.8274000287055969]
FIN

```

## TensorFlow MultiWorkerMirrored

This example is derived
from [TensorFlow official tutorial]( https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras)
to demonstrate how Primus applications integrate with `MultiWorkerMirroredStrategy`.

**To execute**

```shell
$ cd /usr/lib/primus-kubernetes/examples/tensorflow
$ /usr/lib/primus-kubernetes/sbin/primus-submit --primus_conf tensorflow-multiworkermirrored/primus_config.json
...
[2023-02-20 15:00:27:198] [INFO] - com.bytedance.primus.runtime.kubernetesnative.client.KubernetesSubmitCmdRunner.lambda$doWaitAppCompletion$0(KubernetesSubmitCmdRunner.java:151) current driver pod status: Succeeded
[2023-02-20 15:00:27:198] [INFO] - com.bytedance.primus.runtime.kubernetesnative.client.Client.execute(Client.java:56) Shutting down LogManager
```

**To evaluate**

```shell
$ cd /usr/lib/primus-kubernetes/examples/tensorflow
$ bash resources/data/mnist/evaluate.sh resources/data/mnist/test hdfs:///primus/examples/mnist/models/tensorflow-multiworkermirrored
...
313/313 - 0s - loss: 7.3492 - accuracy: 0.8261 - 398ms/epoch - 1ms/step
Model accuracy: [7.349215507507324, 0.8260999917984009]
FIN
```

