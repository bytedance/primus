#!/usr/bin/env bash

#
# Copyright 2022 Bytedance Inc.
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

set -eo pipefail

source __primus_conf__/primus-env.sh
env | sort
echo "================================"

ls -ltr ./
ls -ltr ./__primus_lib__
ls -ltr ./__primus_conf__

"$JAVA_HOME"/bin/java -cp ./*:./__primus_lib__/*:"$CLASSPATH" \
    -Xmx1024m \
    -Dlog4j2.configurationFile=/opt/primus-share/__primus_conf__/log4j2.xml \
    com.bytedance.primus.runtime.kubernetesnative.executor.ContainerMain \
    --am_host="$AM_HOST" \
    --am_port="$AM_PORT" \
    --role="$EXECUTOR_ROLE" \
    --index="$EXECUTOR_INDEX" \
    --uniq_id="$EXECUTOR_UNIQ_ID" \
    --apiserver_host="$AM_APISERVER_HOST" \
    --apiserver_port="$AM_APISERVER_PORT" \
    --conf=__primus_conf__/primus.conf \
    --port_ranges "$PORT_RANGES"

PRIMUS_EXIT_CODE=$?
echo "PRIMUS PROCESS Exit:" ${PRIMUS_EXIT_CODE}

sleep "$SLEEP_SECONDS_BEFORE_POD_EXIT_ENV_KEY"
exit $PRIMUS_EXIT_CODE
