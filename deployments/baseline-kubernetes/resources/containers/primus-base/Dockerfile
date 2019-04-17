FROM ubuntu:22.04

# Install development tools
RUN apt-get update
RUN apt-get install -y vim tree iputils-ping net-tools telnet wget curl dnsutils

# Java will be mounted
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Hadoop will be mounted
ENV HADOOP_HOME=/usr/lib/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH

ENTRYPOINT ["sleep"]
CMD ["43200"]
