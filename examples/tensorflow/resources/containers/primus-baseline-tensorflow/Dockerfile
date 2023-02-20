FROM primus-baseline-base

# Install Python
RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-venv

# Install TensorFlow
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install 'pandas==1.5.3'
RUN python3 -m pip install 'tensorflow==2.8.0'
RUN python3 -m pip install 'tensorflow-io==0.24.0'
RUN python3 -m pip install 'protobuf==3.20.1'

ENTRYPOINT ["sleep"]
CMD ["43200"]
