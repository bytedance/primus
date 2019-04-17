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

export HADOOP_HOME=/usr/lib/hadoop/
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

export YARN_HOME=${HADOOP_HOME}
export YARN_CONF_DIR=${HADOOP_CONF_DIR}

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export CLASSPATH=${JAVA_HOME}/lib:${CLASSPATH}:${HADOOP_HOME}/share/hadoop/*

export PATH=${JAVA_HOME}/bin:${HODOOP_HOME}/bin:${PATH}
source ${HADOOP_HOME}/libexec/hadoop-config.sh
