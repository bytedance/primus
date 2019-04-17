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
cd "$(dirname "$0")"

source __primus_conf__/primus-env.sh
env | sort
echo "================================"

"$JAVA_HOME"/bin/java -cp ./*:./__primus_lib__/*:"$CLASSPATH" \
    -Dlog4j2.configurationFile=/opt/primus-share/__primus_conf__/log4j2.xml \
    -Xmx"$PRIMUS_AM_JAVA_MEMORY_XMX" \
    com.bytedance.primus.runtime.kubernetesnative.am.KubernetesApplicationMasterMain \
    --config __primus_conf__/primus.conf

PRIMUS_EXIT_CODE=$?
echo "PRIMUS PROCESS Exit:" ${PRIMUS_EXIT_CODE}
sleep "$SLEEP_SECONDS_BEFORE_POD_EXIT_ENV_KEY"
exit $PRIMUS_EXIT_CODE
