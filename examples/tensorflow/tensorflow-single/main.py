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
import pandas as pd
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


def main():
    # Init
    args = parser.parse_args()

    # Prepare the input dateset
    training_dataset = tf.data.Dataset.from_generator(
            read_stdin,
            output_types=(
                tf.int32,
                tf.int32
            ),
            output_shapes=(
                tf.TensorShape(784),
                tf.TensorShape(1)
            ))

    # Prepare the model
    model = tf.keras.models.Sequential([
        tf.keras.layers.Input(shape=(784)),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(10)
    ])

    model.compile(optimizer='adam',
                  loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                  metrics=['accuracy'])

    # Start training
    model.fit(training_dataset.batch(1000))

    # Persist the model
    model.save(args.output)


if __name__ == "__main__":
    main()
