# Copyright 2023 Bytedance Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# ==============================================================================

import argparse
import json
import os
import sys
import tensorflow as tf
import tensorflow_io  # Import for side-effects

parser = argparse.ArgumentParser(description='Evaluate a mnist model')
parser.add_argument('--output', dest='output', type=str,
                    default='model.h5',
                    help='path to the output model')


def read_stdin():
    while True:
        line = sys.stdin.readline()
        if line:
            tokens = list(map(int, line.strip().split(',')))
            yield (
                tf.convert_to_tensor(tokens[1:]),
                tf.convert_to_tensor(tokens[:1])
            )
        else:
            return


def create_stdin_dataset():
    return tf.data.Dataset \
            .from_generator(
                read_stdin,
                output_types=(
                    tf.int32,
                    tf.int32
                ),
                output_shapes=(
                    tf.TensorShape(784),
                    tf.TensorShape(1)
                )
            ) \
            .batch(1000)


def _is_chief(task_type, task_id):
    # Note: there are two possible `TF_CONFIG` configuration.
    #   1) In addition to `worker` tasks, a `chief` task type is use;
    #      in this case, this function should be modified to
    #      `return task_type == 'chief'`.
    return (task_type == 'worker' and task_id == 0) or task_type is None


def _get_temp_dir(dirpath, task_id):
    base_dirpath = 'workertemp_' + str(task_id)
    temp_dir = os.path.join(dirpath, base_dirpath)
    tf.io.gfile.makedirs(temp_dir)
    return temp_dir


def write_filepath(filepath, task_type, task_id):
    dirpath = os.path.dirname(filepath)
    base = os.path.basename(filepath)
    if not _is_chief(task_type, task_id):
        dirpath = _get_temp_dir(dirpath, task_id)
    return os.path.join(dirpath, base)


def main():
    # Init
    tf_config = os.environ["TF_CONFIG"]
    tf_config = json.loads(tf_config)

    args = parser.parse_args()

    # Prepare distributed strategy
    strategy = tf.distribute.MultiWorkerMirroredStrategy()

    # Prepare model
    with strategy.scope():
        model = tf.keras.models.Sequential([
            tf.keras.layers.Input(shape=(784)),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(10)
        ])

        model.compile(
                optimizer='adam',
                loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                metrics=['accuracy'])

    # Prepare training dataset
    training_dataset = create_stdin_dataset()

    # Start training
    model.fit(training_dataset, steps_per_epoch=20)

    # Persist the model
    task_type, task_id = (strategy.cluster_resolver.task_type,
                          strategy.cluster_resolver.task_id)

    write_model_path = write_filepath(args.output, task_type, task_id)
    model.save(write_model_path)

    if not _is_chief(task_type, task_id):
        tf.io.gfile.rmtree(os.path.dirname(write_model_path))


if __name__ == '__main__':
    main()

