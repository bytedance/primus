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
import gzip
import os

import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_io  # Import for side-effects

parser = argparse.ArgumentParser(description='Evaluate a mnist model')
parser.add_argument('--mnist', dest='mnist', type=str,
                    default='.',
                    help='path to mnist datasets')
parser.add_argument('--model', dest='model', type=str,
                    default='model.h5',
                    help='path to the model')


DATA_FILENAMES = {
    "test_images": "t10k-images-idx3-ubyte.gz",  # 10,000 test images.
    "test_labels": "t10k-labels-idx1-ubyte.gz",  # 10,000 test labels.
}


def load_images(path: str) -> np.ndarray:
    with tf.io.gfile.GFile(path, "rb") as raw_file:
        with gzip.open(raw_file, "rb") as mnist_file:
            dataset = np \
                .frombuffer(mnist_file.read(), "B", offset=16) \
                .reshape(-1, 784)
    return dataset


def load_labels(path: str) -> np.ndarray:
    with tf.io.gfile.GFile(path, "rb") as raw_file:
        with gzip.open(raw_file, "rb") as mnist_file:
            dataset = np.frombuffer(mnist_file.read(), "B", offset=8)
    return dataset


def load_test_dataset(path: str):
    return (
        load_images(os.path.join(path, DATA_FILENAMES["test_images"])),
        load_labels(os.path.join(path, DATA_FILENAMES["test_labels"]))
    )


def main():
    # Parse CLI arguments
    args = parser.parse_args()

    # Load the model
    model = tf.keras.models.load_model(args.model)

    # Start evaluating
    (x_test, y_test) = load_test_dataset(args.mnist)
    print("Model accuracy: {}".format( model.evaluate(x_test,  y_test, verbose=2)))


if __name__ == "__main__":
    main()
