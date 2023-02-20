#!/bin/bash
set -ex

TOOL_HOME=$(dirname $0)
MNIST=$1
MODEL=$2

# Setup ENV
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server/
export HADOOP_HDFS_HOME=/usr/lib/hadoop
export CLASSPATH="$(/usr/lib/hadoop/bin/hadoop classpath --glob)"

# Unfold and source the portable venv
python3 $TOOL_HOME/evaluate.py --mnist $MNIST --model $MODEL

echo "FIN"
