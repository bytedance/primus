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

# Environment settings
set -euo pipefail
cd "$(dirname "$0")"/../..
echo -e "Workspace: $(pwd)"

PACKAGE_ONLY="false"
SKIP_TESTS="false"

while [[ $# -gt 0 ]]; do
  case $1 in
    --package-only)
      PACKAGE_ONLY="true"
      shift
      ;;
    --skip-tests)
      SKIP_TESTS="true"
      shift
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

# Compile Primus jar
if [[ $PACKAGE_ONLY != "true" ]]; then
  if [[ $SKIP_TESTS == "true" ]]; then
    mvn clean package -Djdk.tls.client.protocols=TLSv1.2 -DskipTests -P stable
  else
    mvn clean package -Djdk.tls.client.protocols=TLSv1.2 -P stable
  fi
fi

# Packaging Baseline Primus
rm -rf output
rm -rf *.tgz

mkdir -p output/jars
cp -f runtime/runtime-kubernetes-native/target/primus-kubernetes-native-STABLE.jar output/jars/primus-STABLE.jar

mkdir -p output/conf
cp -rf deployments/baseline-kubernetes/conf/* output/conf

mkdir -p output/sbin
cp -rf deployments/baseline-kubernetes/sbin/* output/sbin

mkdir -p output/resources
cp -rf deployments/baseline-kubernetes/resources/* output/resources

mkdir -p output/examples
cp -rf examples/* output/examples

mkdir -p output/tests
cp -rf deployments/baseline-kubernetes/tests/* output/tests
