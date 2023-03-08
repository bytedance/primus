# Copyright 2019 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file may have been modified by Beijing Volcanoengine Technology Ltd.
# ==============================================================================

import json
import os
import tensorflow as tf

# Set the environment variable to allow reporting worker and ps failure to the
# coordinator. This is a workaround and won't be necessary in the future.
os.environ["GRPC_FAIL_FAST"] = "use_caller"

# Parse TF_CONFIG
tf_config = os.environ["TF_CONFIG"]
tf_config = json.loads(tf_config)

cluster_spec = tf.train.ClusterSpec(tf_config["cluster"])
task_spec = tf_config["task"]

NUM_PS = len(tf_config["cluster"]["ps"])
NUM_WORKERS = len(tf_config["cluster"]["worker"])

# Creating Cluster
cluster_resolver = tf.distribute.cluster_resolver.SimpleClusterResolver(
    cluster_spec, rpc_layer="grpc")

# Create strategy
variable_partitioner = (
    tf.distribute.experimental.partitioners.MinSizePartitioner(
        min_shard_bytes=(256 << 10),
        max_shards=NUM_PS))

strategy = tf.distribute.experimental.ParameterServerStrategy(
    cluster_resolver,
    variable_partitioner=variable_partitioner)


def dataset_fn(input_context):
    global_batch_size = 64
    batch_size = input_context.get_per_replica_batch_size(global_batch_size)

    x = tf.random.uniform((10, 10))
    y = tf.random.uniform((10,))

    return tf.data.Dataset.from_tensor_slices((x, y)).shuffle(10).repeat() \
        .shard(
        input_context.num_input_pipelines,
        input_context.input_pipeline_id) \
        .batch(batch_size) \
        .prefetch(2)


# Build model
with strategy.scope():
    model = tf.keras.models.Sequential([tf.keras.layers.Dense(10)])
    model.compile(tf.keras.optimizers.SGD(),
                  loss='mse', steps_per_execution=10)

model.fit(
    tf.keras.utils.experimental.DatasetCreator(dataset_fn),
    epochs=5, steps_per_epoch=20)
