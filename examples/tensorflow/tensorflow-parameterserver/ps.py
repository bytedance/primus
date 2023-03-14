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

# Start parameter server
server = tf.distribute.Server(
    cluster_spec,
    job_name=task_spec["type"],
    task_index=task_spec["index"],
    protocol="grpc")

server.join()
