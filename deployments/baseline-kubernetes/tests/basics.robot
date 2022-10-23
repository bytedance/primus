*** Settings ***
Library  Collections
Library  Process
Library  String

*** Variables ***
# Variables to override
${PRIMUS_HOME}  the path to the installed Primus
${HADOOP_HOME}  the path to the installed Hadoop
${KAFKA_HOME}   the path to the installed Kafka

${KUBERNETES_APPLICATION_ID_EXTRACT_PATTERN}   kubectl -n primus logs ([a-zA-Z0-9-_]+)
${KUBERNETES_APPLICATION_URL_EXTRACT_PATTERN}  Kubernetes AM tracking URL: (http://[a-zA-Z0-9-_/.:]+)

*** Keywords ***
prepare hdfs data  [Arguments]  ${src}  ${dst}
                   Log  preparing HDFS data: ${src} -> ${dst}
                   Run Process  ${HADOOP_HOME}/bin/hdfs  dfs  -rm  -r  ${dst}
                   Run Process  ${HADOOP_HOME}/bin/hdfs  dfs  -mkdir  -p  ${dst}
                   Run Process  ${HADOOP_HOME}/bin/hdfs  dfs  -put  ${src}  ${dst}

submit primus application   [Arguments]  ${primus_conf}
                            Log  Submitting Primus Application: ${primus_conf}
                            ${uuid} =  Evaluate  uuid.uuid4()  modules=uuid
                            ${client_log_file} =  Catenate  ${uuid}.logs

                            Run Process  ${PRIMUS_HOME}/sbin/primus-submit  --primus_conf  ${primus_conf}  stdout=${client_log_file}
                            command result contains  cat ${client_log_file} | grep current  current driver pod status: Succeeded

                            Log  Extracting application ID
                            ${application_id} =  command result matches  cat ${client_log_file} | grep kubectl  ${KUBERNETES_APPLICATION_ID_EXTRACT_PATTERN}
                            [Return]  ${application_id}

submit primus application async   [Arguments]  ${primus_conf}
                                  Log  Submitting Primus Application in background: ${primus_conf}

                                  ${uuid} =  Evaluate  uuid.uuid4()  modules=uuid
                                  ${client_log_file} =  Catenate  ${uuid}.logs

                                  ${process} =  Start Process  ${PRIMUS_HOME}/sbin/primus-submit  --primus_conf  ${primus_conf}  stdout=${client_log_file}
                                  ${application_id} =   wait until command result matches  cat ${client_log_file}  ${KUBERNETES_APPLICATION_ID_EXTRACT_PATTERN}
                                  ${application_url} =  wait until command result matches  cat ${client_log_file}  ${KUBERNETES_APPLICATION_URL_EXTRACT_PATTERN}

                                  [Return]  ${process}  ${application_id}  ${application_url}

kill primus application and wait for client  [Arguments]  ${process}  ${application_id}
                                             Log  Killing Primus application of ${application_id}
                                             Run Process  kubectl  -n  primus  delete  pod  ${application_id}
                                             ${result} =  Wait For Process  ${process}
                                             Should Contain  ${result.stdout}  current driver pod status: Failed

grep primus application logs  [Arguments]  ${application_id}  ${process_cmd}
                              Log  Checking the logs of ${application_id}
                              ${logs} =  Run Process  bash  -c  kubectl -n primus logs -l primus.k8s.io/app-selector\=${application_id} --tail\=-1 | ${process_cmd}
                              ${splitted} =  Split To Lines  ${logs.stdout}
                              [Return]  ${splitted}

wait until command result matches  [Arguments]  ${command}  ${pattern}
                                   ${result} =  Wait Until Keyword Succeeds  2 min  10 sec  command result matches  ${command}  ${pattern}
                                   [Return]  ${result}

command result matches  [Arguments]  ${command}  ${pattern}
                        ${result} =  Run Process  bash  -c  ${command}
                        ${match}  ${captured} =  Should Match Regexp  ${result.stdout}  ${pattern}
                        [Return]  ${captured}

wait until command result contains  [Arguments]  ${command}  ${content}
                                    ${result} =  Wait Until Keyword Succeeds  2 min  10 sec  command result contains  ${command}  ${content}
                                    [Return]  ${result}

command result contains  [Arguments]  ${command}  ${content}
                         ${result} =  Run Process  bash  -c  ${command}
                         Should Contain  ${result.stdout}  ${content}

*** Test Cases ***
hello-primus
  # Submit Primus application and check Primus client logs
  ${application_id} =  submit primus application  ${PRIMUS_HOME}/examples/hello/primus_config.json

  # Check Primus application logs
  ${hello_captured} =  grep Primus Application Logs  ${application_id}  grep -E ^Hello
  ${hello_expected} =  Create List
  Append To List  ${hello_expected}  Hello from Chief
  Append To List  ${hello_expected}  Hello from PS
  Append To List  ${hello_expected}  Hello from Worker

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}  ignore_order=True


primus-ui
   ${process}  ${application_id}  ${application_url} =  submit primus application async  ${PRIMUS_HOME}/examples/hello-extended/primus_config.json

   # Primus AM UI
   # TODO: Test UI with Selenium
   # - Test downloading primus conf
   # - Test retrieving AM Logs
   # - Test retrieving container logs
   wait until command result contains  curl ${application_url}webapps/primus/status.json | gunzip -  primusConf
   wait until command result contains  curl ${application_url}webapps/primus/status.json | gunzip -  primus-hello-extended
   wait until command result contains  curl ${application_url}webapps/primus/status.json | gunzip -  Hello from Chief
   wait until command result contains  curl ${application_url}webapps/primus/status.json | gunzip -  Hello from PS
   wait until command result contains  curl ${application_url}webapps/primus/status.json | gunzip -  Hello from Worker

   # Primus History UI
   # TODO: Test UI with Selenium
   # - Test downloading primus conf
   # - Test retrieving AM Logs
   # - Test retrieving container logs
   wait until command result contains  curl http://localhost:7890/app/${application_id}/status.json | gunzip -  primusConf
   wait until command result contains  curl http://localhost:7890/app/${application_id}/status.json | gunzip -  primus-hello-extended
   wait until command result contains  curl http://localhost:7890/app/${application_id}/status.json | gunzip -  Hello from Chief
   wait until command result contains  curl http://localhost:7890/app/${application_id}/status.json | gunzip -  Hello from PS
   wait until command result contains  curl http://localhost:7890/app/${application_id}/status.json | gunzip -  Hello from Worker

   # Teardown
   kill primus application and wait for client  ${process}  ${application_id}


input-file-text
  # Prepare test data
  prepare hdfs data  ${PRIMUS_HOME}/examples/input-file-text/data  /primus/examples/input-file-text/

  # Submit Primus application and check Primus client logs
  ${application_id} =  Submit Primus Application  ${PRIMUS_HOME}/examples/input-file-text/primus_config.json

  # Check Primus application logs
  ${hello_captured} =  grep Primus Application Logs  ${application_id}  grep "Hello from input-file-text"
  ${hello_expected} =  Create List
  Append To List  ${hello_expected}  0\tHello from input-file-text: 0-0
  Append To List  ${hello_expected}  32\tHello from input-file-text: 0-1
  Append To List  ${hello_expected}  64\tHello from input-file-text: 0-2
  Append To List  ${hello_expected}  96\tHello from input-file-text: 0-3
  Append To List  ${hello_expected}  128\tHello from input-file-text: 0-4
  Append To List  ${hello_expected}  160\tHello from input-file-text: 0-5
  Append To List  ${hello_expected}  192\tHello from input-file-text: 0-6
  Append To List  ${hello_expected}  224\tHello from input-file-text: 0-7
  Append To List  ${hello_expected}  0\tHello from input-file-text: 1-0
  Append To List  ${hello_expected}  32\tHello from input-file-text: 1-1
  Append To List  ${hello_expected}  64\tHello from input-file-text: 1-2
  Append To List  ${hello_expected}  96\tHello from input-file-text: 1-3
  Append To List  ${hello_expected}  128\tHello from input-file-text: 1-4
  Append To List  ${hello_expected}  160\tHello from input-file-text: 1-5
  Append To List  ${hello_expected}  192\tHello from input-file-text: 1-6
  Append To List  ${hello_expected}  224\tHello from input-file-text: 1-7
  Append To List  ${hello_expected}  0\tHello from input-file-text: 2-0
  Append To List  ${hello_expected}  32\tHello from input-file-text: 2-1
  Append To List  ${hello_expected}  64\tHello from input-file-text: 2-2
  Append To List  ${hello_expected}  96\tHello from input-file-text: 2-3
  Append To List  ${hello_expected}  128\tHello from input-file-text: 2-4
  Append To List  ${hello_expected}  160\tHello from input-file-text: 2-5
  Append To List  ${hello_expected}  192\tHello from input-file-text: 2-6
  Append To List  ${hello_expected}  224\tHello from input-file-text: 2-7
  Append To List  ${hello_expected}  0\tHello from input-file-text: 3-0
  Append To List  ${hello_expected}  32\tHello from input-file-text: 3-1
  Append To List  ${hello_expected}  64\tHello from input-file-text: 3-2
  Append To List  ${hello_expected}  96\tHello from input-file-text: 3-3
  Append To List  ${hello_expected}  128\tHello from input-file-text: 3-4
  Append To List  ${hello_expected}  160\tHello from input-file-text: 3-5
  Append To List  ${hello_expected}  192\tHello from input-file-text: 3-6
  Append To List  ${hello_expected}  224\tHello from input-file-text: 3-7

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}  ignore_order=True


input-file-text-timerange
  # Prepare test data
  prepare hdfs data  ${PRIMUS_HOME}/examples/input-file-text-timerange/data  /primus/examples/input-file-text-timerange/

  # Submit Primus application and check Primus client logs
  ${application_id} =  Submit Primus Application  ${PRIMUS_HOME}/examples/input-file-text-timerange/primus_config.json

  # Check Primus application logs
  ${hello_captured} =  grep Primus Application Logs  ${application_id}  grep "Hello from input-file-text-timerange"
  ${hello_expected} =  Create List
  Append To List  ${hello_expected}  0\tHello from input-file-text-timerange: 2020-01-01 - 0
  Append To List  ${hello_expected}  53\tHello from input-file-text-timerange: 2020-01-01 - 1
  Append To List  ${hello_expected}  106\tHello from input-file-text-timerange: 2020-01-01 - 2
  Append To List  ${hello_expected}  159\tHello from input-file-text-timerange: 2020-01-01 - 3
  Append To List  ${hello_expected}  0\tHello from input-file-text-timerange: 2020-01-02 - 0
  Append To List  ${hello_expected}  53\tHello from input-file-text-timerange: 2020-01-02 - 1
  Append To List  ${hello_expected}  106\tHello from input-file-text-timerange: 2020-01-02 - 2
  Append To List  ${hello_expected}  159\tHello from input-file-text-timerange: 2020-01-02 - 3
  Append To List  ${hello_expected}  0\tHello from input-file-text-timerange: 2020-01-03 - 0
  Append To List  ${hello_expected}  53\tHello from input-file-text-timerange: 2020-01-03 - 1
  Append To List  ${hello_expected}  106\tHello from input-file-text-timerange: 2020-01-03 - 2
  Append To List  ${hello_expected}  159\tHello from input-file-text-timerange: 2020-01-03 - 3
  Append To List  ${hello_expected}  0\tHello from input-file-text-timerange: 2020-01-04 - 0
  Append To List  ${hello_expected}  53\tHello from input-file-text-timerange: 2020-01-04 - 1
  Append To List  ${hello_expected}  106\tHello from input-file-text-timerange: 2020-01-04 - 2
  Append To List  ${hello_expected}  159\tHello from input-file-text-timerange: 2020-01-04 - 3

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}  ignore_order=True


input-kafka-text
  # Obtain docker host IP
  ${docker_host_ip} =  Run Process  bash  -c  ip -f inet addr show docker0 | sed -En -e 's/.*inet ([0-9.:]+).*/\\1/p'

  # Prepare Kafka topic with data
  Log  Preparing test data
  Run Process  ${KAFKA_HOME}/bin/kafka-topics.sh  --bootstrap-server  ${docker_host_ip.stdout}:9092  --delete  --topic  primus-example-input-kafka-text
  Run Process  ${KAFKA_HOME}/bin/kafka-topics.sh  --bootstrap-server  ${docker_host_ip.stdout}:9092  --create  --topic  primus-example-input-kafka-text
  Run Process  bash  -c  cat ${PRIMUS_HOME}/examples/input-kafka-text/data/* | ${KAFKA_HOME}/bin/kafka-console-producer.sh --bootstrap-server ${docker_host_ip.stdout}:9092 --topic primus-example-input-kafka-text

  # Submit Primus application and check Primus client logs
  Run Process  bash  -c  sed 's/<kafka-broker-ip:port>/${docker_host_ip.stdout}:9092/' ${PRIMUS_HOME}/examples/input-kafka-text/primus_config.json > ${PRIMUS_HOME}/examples/input-kafka-text/primus_config.json.patched
  ${application_id} =  Submit Primus Application  ${PRIMUS_HOME}/examples/input-kafka-text/primus_config.json.patched

  # Check Primus application logs
  ${hello_captured} =  grep Primus Application Logs  ${application_id}  grep "Hello from input-kafka-text"
  ${hello_expected} =  Create List
  Append To List  ${hello_expected}  Hello from input-kafka-text: 0-0
  Append To List  ${hello_expected}  Hello from input-kafka-text: 0-1
  Append To List  ${hello_expected}  Hello from input-kafka-text: 0-2
  Append To List  ${hello_expected}  Hello from input-kafka-text: 0-3
  Append To List  ${hello_expected}  Hello from input-kafka-text: 0-4
  Append To List  ${hello_expected}  Hello from input-kafka-text: 0-5
  Append To List  ${hello_expected}  Hello from input-kafka-text: 0-6
  Append To List  ${hello_expected}  Hello from input-kafka-text: 0-7
  Append To List  ${hello_expected}  Hello from input-kafka-text: 1-0
  Append To List  ${hello_expected}  Hello from input-kafka-text: 1-1
  Append To List  ${hello_expected}  Hello from input-kafka-text: 1-2
  Append To List  ${hello_expected}  Hello from input-kafka-text: 1-3
  Append To List  ${hello_expected}  Hello from input-kafka-text: 1-4
  Append To List  ${hello_expected}  Hello from input-kafka-text: 1-5
  Append To List  ${hello_expected}  Hello from input-kafka-text: 1-6
  Append To List  ${hello_expected}  Hello from input-kafka-text: 1-7
  Append To List  ${hello_expected}  Hello from input-kafka-text: 2-0
  Append To List  ${hello_expected}  Hello from input-kafka-text: 2-1
  Append To List  ${hello_expected}  Hello from input-kafka-text: 2-2
  Append To List  ${hello_expected}  Hello from input-kafka-text: 2-3
  Append To List  ${hello_expected}  Hello from input-kafka-text: 2-4
  Append To List  ${hello_expected}  Hello from input-kafka-text: 2-5
  Append To List  ${hello_expected}  Hello from input-kafka-text: 2-6
  Append To List  ${hello_expected}  Hello from input-kafka-text: 2-7
  Append To List  ${hello_expected}  Hello from input-kafka-text: 3-0
  Append To List  ${hello_expected}  Hello from input-kafka-text: 3-1
  Append To List  ${hello_expected}  Hello from input-kafka-text: 3-2
  Append To List  ${hello_expected}  Hello from input-kafka-text: 3-3
  Append To List  ${hello_expected}  Hello from input-kafka-text: 3-4
  Append To List  ${hello_expected}  Hello from input-kafka-text: 3-5
  Append To List  ${hello_expected}  Hello from input-kafka-text: 3-6
  Append To List  ${hello_expected}  Hello from input-kafka-text: 3-7

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}  ignore_order=True
