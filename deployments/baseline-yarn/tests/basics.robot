*** Settings ***
Library  Collections
Library  Process
Library  String

*** Variables ***
# Variables to override
${PRIMUS_HOME}  the path to the installed Primus
${HADOOP_HOME}  the path to the installed Hadoop
${KAFKA_HOME}   the path to the installed Kafka

${PRIMUS_EXAMPLE_BASICS}  ${PRIMUS_HOME}/examples/basics

${YARN_APPLICATION_ID_EXTRACT_PATTERN}  INFO impl.YarnClientImpl: Submitted application ([a-zA-Z0-9_]+)
${YARN_APPLICATION_URL_EXTRACT_PATTERN}  INFO client.YarnSubmitCmdRunner: Tracking URL: (http://[a-zA-Z0-9-_/.:]+)

*** Keywords ***
delete hdfs directory  [Arguments]  ${directory}
                       Log  deleting HDFS directory: ${directory}
                       Run Process  ${HADOOP_HOME}/bin/hdfs  dfs  -rm  -r  ${directory}

prepare hdfs data  [Arguments]  ${src}  ${dst}
                   Log  preparing HDFS data: ${src} -> ${dst}
                   Run Process  ${HADOOP_HOME}/bin/hdfs  dfs  -mkdir  -p  ${dst}
                   Run Process  ${HADOOP_HOME}/bin/hdfs  dfs  -put  -f  ${src}  ${dst}

submit primus application   [Arguments]  ${primus_conf}
                            Log  Submitting Primus Application: ${primus_conf}
                            ${result} =  Run Process  ${PRIMUS_HOME}/sbin/primus-submit  --primus_conf  ${primus_conf}
                            Should Contain  ${result.stderr}  Final Application Status: SUCCEEDED

                            Log  Extracting YARN application ID
                            ${yarn_application_id_matches} =  Get Regexp Matches  ${result.stderr}  ${YARN_APPLICATION_ID_EXTRACT_PATTERN}  1
                            ${yarn_application_id} =  Get From List  ${yarn_application_id_matches}  0

                            [Return]  ${yarn_application_id}

submit primus application async   [Arguments]  ${primus_conf}
                                  Log  Submitting Primus Application in background: ${primus_conf}

                                  ${uuid} =  Evaluate  uuid.uuid4()  modules=uuid
                                  ${client_log_file} =  Catenate  ${uuid}.logs

                                  ${process} =  Start Process  ${PRIMUS_HOME}/sbin/primus-submit  --primus_conf  ${primus_conf}  stderr=${client_log_file}
                                  ${yarn_application_id} =   wait until command result matches  cat ${client_log_file}  ${YARN_APPLICATION_ID_EXTRACT_PATTERN}
                                  ${yarn_application_url} =  wait until command result matches  cat ${client_log_file}  ${YARN_APPLICATION_URL_EXTRACT_PATTERN}

                                  [Return]  ${process}  ${yarn_application_id}  ${yarn_application_url}

kill primus application and wait for client  [Arguments]  ${process}  ${application_id}
                                             Log  Killing Primus application of ${application_id}
                                             Run Process  ${HADOOP_HOME}/bin/yarn  application  -kill  ${application_id}
                                             ${result} =  Wait For Process  ${process}
                                             Should Contain  ${result.stderr}  Final Application Status: KILLED

grep primus application logs  [Arguments]  ${yarn_application_id}  ${process_cmd}
                              Log  Checking the logs of ${yarn_application_id}
                              Run Process  ${HADOOP_HOME}/bin/yarn  logs  --applicationId  ${yarn_application_id}  stdout=${yarn_application_id}.logs
                              ${processed} =  Run Process  bash  -c  cat ${yarn_application_id}.logs | ${process_cmd}
                              ${splitted} =  Split To Lines  ${processed.stdout}
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
  ${application_id} =  submit primus application  ${PRIMUS_EXAMPLE_BASICS}/hello/primus_config.json

  # Check Primus application logs
  ${hello_captured} =  grep Primus Application Logs  ${application_id}  grep -E ^Hello
  ${hello_expected} =  Create List
  Append To List  ${hello_expected}  Hello from Chief
  Append To List  ${hello_expected}  Hello from PS
  Append To List  ${hello_expected}  Hello from Worker

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}  ignore_order=True


primus-ui
  ${process}  ${application_id}  ${yarn_application_url} =  submit primus application async  ${PRIMUS_EXAMPLE_BASICS}/hello-extended/primus_config.json

  # Primus AM UI
  # TODO: Test UI with Selenium
  # - Test YARN UI redirection
  # - Test downloading primus conf
  # - Test retrieving AM Logs
  # - Test retrieving container logs
  wait until command result contains  curl ${yarn_application_url}webapps/primus/status.json  primusConf
  wait until command result contains  curl ${yarn_application_url}webapps/primus/status.json  primus-hello-extended
  wait until command result contains  curl ${yarn_application_url}webapps/primus/status.json  Hello from Chief
  wait until command result contains  curl ${yarn_application_url}webapps/primus/status.json  Hello from PS
  wait until command result contains  curl ${yarn_application_url}webapps/primus/status.json  Hello from Worker

  # Primus History UI
  # TODO: Test UI with Selenium
  # - Test YARN UI redirection
  # - Test downloading primus conf
  # - Test retrieving AM Logs
  # - Test retrieving container logs
  wait until command result contains  curl http://localhost:17890/app/${application_id}/status.json | gunzip -  primusConf
  wait until command result contains  curl http://localhost:17890/app/${application_id}/status.json | gunzip -  primus-hello-extended
  wait until command result contains  curl http://localhost:17890/app/${application_id}/status.json | gunzip -  Hello from Chief
  wait until command result contains  curl http://localhost:17890/app/${application_id}/status.json | gunzip -  Hello from PS
  wait until command result contains  curl http://localhost:17890/app/${application_id}/status.json | gunzip -  Hello from Worker

  # Teardown
  kill primus application and wait for client  ${process}  ${application_id}


input-file-customized
  # Prepare test data
  prepare hdfs data  ${PRIMUS_EXAMPLE_BASICS}/input-file-customized/data  /primus/examples/input-file-customized/

  # Submit Primus application and check Primus client logs
  ${application_id} =  Submit Primus Application  ${PRIMUS_EXAMPLE_BASICS}/input-file-customized/primus_config.json

  # Check Primus application logs
  ${hello_captured} =  grep Primus Application Logs  ${application_id}  grep -a "Hello from input-file-customized"
  ${hello_expected} =  Create List
  Append To List  ${hello_expected}  KP-0-KS\tVP-Hello from input-file-customized: 0-0-VS
  Append To List  ${hello_expected}  KP-38-KS\tVP-Hello from input-file-customized: 0-1-VS
  Append To List  ${hello_expected}  KP-76-KS\tVP-Hello from input-file-customized: 0-2-VS
  Append To List  ${hello_expected}  KP-114-KS\tVP-Hello from input-file-customized: 0-3-VS
  Append To List  ${hello_expected}  KP-0-KS\tVP-Hello from input-file-customized: 1-0-VS
  Append To List  ${hello_expected}  KP-38-KS\tVP-Hello from input-file-customized: 1-1-VS
  Append To List  ${hello_expected}  KP-76-KS\tVP-Hello from input-file-customized: 1-2-VS
  Append To List  ${hello_expected}  KP-114-KS\tVP-Hello from input-file-customized: 1-3-VS
  Append To List  ${hello_expected}  KP-0-KS\tVP-Hello from input-file-customized: 2-0-VS
  Append To List  ${hello_expected}  KP-38-KS\tVP-Hello from input-file-customized: 2-1-VS
  Append To List  ${hello_expected}  KP-76-KS\tVP-Hello from input-file-customized: 2-2-VS
  Append To List  ${hello_expected}  KP-114-KS\tVP-Hello from input-file-customized: 2-3-VS
  Append To List  ${hello_expected}  KP-0-KS\tVP-Hello from input-file-customized: 3-0-VS
  Append To List  ${hello_expected}  KP-38-KS\tVP-Hello from input-file-customized: 3-1-VS
  Append To List  ${hello_expected}  KP-76-KS\tVP-Hello from input-file-customized: 3-2-VS
  Append To List  ${hello_expected}  KP-114-KS\tVP-Hello from input-file-customized: 3-3-VS

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}


input-file-text
  # Prepare test data
  prepare hdfs data  ${PRIMUS_EXAMPLE_BASICS}/input-file-text/data  /primus/examples/input-file-text/

  # Submit Primus application and check Primus client logs
  ${application_id} =  Submit Primus Application  ${PRIMUS_EXAMPLE_BASICS}/input-file-text/primus_config.json

  # Check Primus application logs
  ${hello_captured} =  grep Primus Application Logs  ${application_id}  grep "Hello from input-file-text"
  ${hello_expected} =  Create List
  Append To List  ${hello_expected}  Hello from input-file-text: 0-0
  Append To List  ${hello_expected}  Hello from input-file-text: 0-1
  Append To List  ${hello_expected}  Hello from input-file-text: 0-2
  Append To List  ${hello_expected}  Hello from input-file-text: 0-3
  Append To List  ${hello_expected}  Hello from input-file-text: 0-4
  Append To List  ${hello_expected}  Hello from input-file-text: 0-5
  Append To List  ${hello_expected}  Hello from input-file-text: 0-6
  Append To List  ${hello_expected}  Hello from input-file-text: 0-7
  Append To List  ${hello_expected}  Hello from input-file-text: 1-0
  Append To List  ${hello_expected}  Hello from input-file-text: 1-1
  Append To List  ${hello_expected}  Hello from input-file-text: 1-2
  Append To List  ${hello_expected}  Hello from input-file-text: 1-3
  Append To List  ${hello_expected}  Hello from input-file-text: 1-4
  Append To List  ${hello_expected}  Hello from input-file-text: 1-5
  Append To List  ${hello_expected}  Hello from input-file-text: 1-6
  Append To List  ${hello_expected}  Hello from input-file-text: 1-7
  Append To List  ${hello_expected}  Hello from input-file-text: 2-0
  Append To List  ${hello_expected}  Hello from input-file-text: 2-1
  Append To List  ${hello_expected}  Hello from input-file-text: 2-2
  Append To List  ${hello_expected}  Hello from input-file-text: 2-3
  Append To List  ${hello_expected}  Hello from input-file-text: 2-4
  Append To List  ${hello_expected}  Hello from input-file-text: 2-5
  Append To List  ${hello_expected}  Hello from input-file-text: 2-6
  Append To List  ${hello_expected}  Hello from input-file-text: 2-7
  Append To List  ${hello_expected}  Hello from input-file-text: 3-0
  Append To List  ${hello_expected}  Hello from input-file-text: 3-1
  Append To List  ${hello_expected}  Hello from input-file-text: 3-2
  Append To List  ${hello_expected}  Hello from input-file-text: 3-3
  Append To List  ${hello_expected}  Hello from input-file-text: 3-4
  Append To List  ${hello_expected}  Hello from input-file-text: 3-5
  Append To List  ${hello_expected}  Hello from input-file-text: 3-6
  Append To List  ${hello_expected}  Hello from input-file-text: 3-7

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}


input-file-text-checkpoint
  # Prepare test data
  delete hdfs directory  /primus/examples/input-file-text-checkpoint/savepoint/
  prepare hdfs data  ${PRIMUS_EXAMPLE_BASICS}/input-file-text-checkpoint/data  /primus/examples/input-file-text-checkpoint/

  # First run - write checkpoints and task files
  ${application_id} =  Submit Primus Application  ${PRIMUS_EXAMPLE_BASICS}/input-file-text-checkpoint/primus_config.json
  ${first_captured} =  grep Primus Application Logs  ${application_id}  grep "Hello from input-file-text-checkpoint"
  ${first_expected} =  Create List
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 0-0
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 0-1
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 0-2
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 0-3
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 0-4
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 0-5
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 0-6
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 0-7
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 1-0
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 1-1
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 1-2
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 1-3
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 1-4
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 1-5
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 1-6
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 1-7
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 2-0
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 2-1
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 2-2
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 2-3
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 2-4
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 2-5
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 2-6
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 2-7
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 3-0
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 3-1
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 3-2
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 3-3
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 3-4
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 3-5
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 3-6
  Append To List  ${first_expected}  Hello from input-file-text-checkpoint: 3-7

  Lists Should Be Equal  ${first_captured}  ${first_expected}

  # Second run - continue from first run and thus no data is read by Primus
  ${application_id} =  Submit Primus Application  ${PRIMUS_EXAMPLE_BASICS}/input-file-text-checkpoint/primus_config.json
  ${second_captured} =  grep Primus Application Logs  ${application_id}  grep "Hello from input-file-text-checkpoint"
  ${second_expected} =  Create List

  Lists Should Be Equal  ${second_captured}  ${second_expected}

  # Third run - inject a checkpoint to start from middle
  prepare hdfs data  ${PRIMUS_EXAMPLE_BASICS}/input-file-text-checkpoint/savepoint  /primus/examples/input-file-text-checkpoint/
  ${application_id} =  Submit Primus Application  ${PRIMUS_EXAMPLE_BASICS}/input-file-text-checkpoint/primus_config.json
  ${third_captured} =  grep Primus Application Logs  ${application_id}  grep "Hello from input-file-text-checkpoint"
  ${third_expected} =  Create List
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 2-0
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 2-1
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 2-2
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 2-3
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 2-4
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 2-5
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 2-6
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 2-7
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 3-0
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 3-1
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 3-2
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 3-3
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 3-4
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 3-5
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 3-6
  Append To List  ${third_expected}  Hello from input-file-text-checkpoint: 3-7

  Lists Should Be Equal  ${third_captured}  ${third_expected}


input-file-text-timerange
  # Prepare test data
  prepare hdfs data  ${PRIMUS_EXAMPLE_BASICS}/input-file-text-timerange/data  /primus/examples/input-file-text-timerange/

  # Submit Primus application and check Primus client logs
  ${application_id} =  Submit Primus Application  ${PRIMUS_EXAMPLE_BASICS}/input-file-text-timerange/primus_config.json

  # Check Primus application logs
  ${hello_captured} =  grep Primus Application Logs  ${application_id}  grep "Hello from input-file-text-timerange"
  ${hello_expected} =  Create List
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-01 - 0
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-01 - 1
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-01 - 2
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-01 - 3
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(hourly): 2020-01-01-00 - 0
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(hourly): 2020-01-01-00 - 1
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(hourly): 2020-01-01-00 - 2
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(hourly): 2020-01-01-00 - 3
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(hourly): 2020-01-01-01 - 0
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(hourly): 2020-01-01-01 - 1
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(hourly): 2020-01-01-01 - 2
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(hourly): 2020-01-01-01 - 3
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-02 - 0
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-02 - 1
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-02 - 2
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-02 - 3
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-03 - 0
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-03 - 1
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-03 - 2
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-03 - 3
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-04 - 0
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-04 - 1
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-04 - 2
  Append To List  ${hello_expected}  Hello from input-file-text-timerange(daily): 2020-01-04 - 3

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}


input-kafka-text
  # Obtain docker host IP
  ${docker_host_ip} =  Run Process  bash  -c  ip -f inet addr show docker0 | sed -En -e 's/.*inet ([0-9.:]+).*/\\1/p'

  # Prepare Kafka topic with data
  Log  Preparing test data
  Run Process  ${KAFKA_HOME}/bin/kafka-topics.sh  --bootstrap-server  ${docker_host_ip.stdout}:9092  --delete  --topic  primus-example-input-kafka-text
  Run Process  ${KAFKA_HOME}/bin/kafka-topics.sh  --bootstrap-server  ${docker_host_ip.stdout}:9092  --create  --topic  primus-example-input-kafka-text
  Run Process  bash  -c  cat ${PRIMUS_EXAMPLE_BASICS}/input-kafka-text/data/* | ${KAFKA_HOME}/bin/kafka-console-producer.sh --bootstrap-server ${docker_host_ip.stdout}:9092 --topic primus-example-input-kafka-text

  # Submit Primus application and check Primus client logs
  Run Process  bash  -c  sed 's/<kafka-broker-ip:port>/${docker_host_ip.stdout}:9092/' ${PRIMUS_EXAMPLE_BASICS}/input-kafka-text/primus_config.json > ${PRIMUS_EXAMPLE_BASICS}/input-kafka-text/primus_config.json.patched
  ${application_id} =  Submit Primus Application  ${PRIMUS_EXAMPLE_BASICS}/input-kafka-text/primus_config.json.patched

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

  Lists Should Be Equal  ${hello_captured}  ${hello_expected}
