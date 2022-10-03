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

set -euo pipefail
cd "$(dirname "$0")"/../..

USERNAME=$1

TEMP_DIR=deployments/baseline-common/temp
mkdir -p $TEMP_DIR

apt-get update
apt-get install -y \
  vim \
  git \
  wget \
  curl \
  net-tools \
  openjdk-8-jdk \
  maven

# Docker - https://docs.docker.com/engine/install/ubuntu/
echo "Installing Docker..."

apt-get update
apt-get install -y \
  ca-certificates \
  curl \
  gnupg \
  lsb-release

mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install -y \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-compose-plugin

DOCKER_NETWORK_INTERFACE=docker0
DOCKER_HOST_IP=$(ip -f inet addr show docker0 | sed -En -e 's/.*inet ([0-9.:]+).*/\1/p')

echo "Docker network interface: " "$DOCKER_NETWORK_INTERFACE"
echo "Docker host IP:" "$DOCKER_HOST_IP"

getent group docker || groupadd docker
usermod -aG docker $USERNAME

# Hadoop(HDFS and YARN) - https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html
echo "Installing Hadoop(HDFS and YARN)..."

HADOOP_HOME=/usr/lib/hadoop
HADOOP_CONF_DIR="$HADOOP_HOME"/etc/hadoop

ssh-keygen -t rsa -P '' -N '' -f ~/.ssh/id_rsa || true
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz -O $TEMP_DIR/hadoop.tar.gz
tar -zxf $TEMP_DIR/hadoop.tar.gz -C $TEMP_DIR
rm -rf $TEMP_DIR/hadoop
mv $TEMP_DIR/hadoop-3.3.1 $TEMP_DIR/hadoop
mv $TEMP_DIR/hadoop /usr/lib/
cp -r "$HADOOP_CONF_DIR" "$HADOOP_CONF_DIR".bak

cp deployments/baseline-common/hadoop/configs/* "$HADOOP_CONF_DIR"
sed -i "s|DOCKER-HOST-IP|$DOCKER_HOST_IP|" "$HADOOP_CONF_DIR"/core-site.xml
sed -i "s|# export JAVA_HOME=|export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64|" "$HADOOP_CONF_DIR"/hadoop-env.sh
cat <<EOF >> "$HADOOP_CONF_DIR"/hadoop-env.sh
export HDFS_NAMENODE_USER="root"
export HDFS_DATANODE_USER="root"
export HDFS_SECONDARYNAMENODE_USER="root"
export YARN_RESOURCEMANAGER_USER="root"
export YARN_NODEMANAGER_USER="root"
EOF

mkdir -p /app/hadoop/hdfs/nn
mkdir -p /app/hadoop/hdfs/dn
/usr/lib/hadoop/bin/hdfs namenode -format

cp deployments/baseline-common/hadoop/systemd/* /etc/systemd/system/
systemctl enable hdfs yarn yarn-jobhistory
systemctl start  hdfs yarn yarn-jobhistory

/usr/lib/hadoop/bin/hdfs dfs -mkdir -p /user/"$USERNAME"
/usr/lib/hadoop/bin/hdfs dfs -chown -R "$USERNAME" /user/"$USERNAME"

/usr/lib/hadoop/bin/hdfs dfs -mkdir -p /primus
/usr/lib/hadoop/bin/hdfs dfs -mkdir -p /primus/staging
/usr/lib/hadoop/bin/hdfs dfs -mkdir -p /primus/event
/usr/lib/hadoop/bin/hdfs dfs -mkdir -p /primus/history
/usr/lib/hadoop/bin/hdfs dfs -chown -R "$USERNAME" /primus

/usr/lib/hadoop/bin/hdfs dfs -chmod 777 /tmp

# Kafka - https://kafka.apache.org/quickstart
echo "Installing Kafka..."

wget https://archive.apache.org/dist/kafka/3.1.0/kafka_2.13-3.1.0.tgz -O $TEMP_DIR/kafka.tgz
tar -zxf $TEMP_DIR/kafka.tgz -C $TEMP_DIR
rm -rf $TEMP_DIR/kafka
mv $TEMP_DIR/kafka_2.13-3.1.0 $TEMP_DIR/kafka
mv $TEMP_DIR/kafka /usr/lib/

sed -i "s|#listeners=PLAINTEXT://:9092|listeners=PLAINTEXT://$DOCKER_HOST_IP:9092|" /usr/lib/kafka/config/server.properties

cp deployments/baseline-common/kafka/systemd/* /etc/systemd/system/
systemctl enable zookeeper kafka
systemctl start  zookeeper kafka

# kubectl - https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/#install-using-native-package-management
echo "Installing kubectl..."

apt-get update
apt-get install -y ca-certificates curl

curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg
echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | sudo tee /etc/apt/sources.list.d/kubernetes.list

apt-get update
apt-get install -y kubectl

# Kind - https://kind.sigs.k8s.io/docs/user/quick-start/#installing-from-release-binaries
echo "Installing kind..."

curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.14.0/kind-linux-amd64
chmod +x ./kind
mv ./kind /usr/local/bin/kind

echo "Creating Kubernetes cluster..."
runuser -u $USERNAME -- kind create cluster --config=deployments/baseline-common/kubernetes/configs/cluster.yaml

# Robot Framework - https://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html#installing-using-pip
apt-get install -y python3-pip
runuser -u $USERNAME pip install robotframework docutils
