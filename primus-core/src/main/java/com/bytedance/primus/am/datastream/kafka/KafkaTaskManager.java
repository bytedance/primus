/*
 * Copyright 2022 Bytedance Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.primus.am.datastream.kafka;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.controller.SuspendStatusEnum;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.TaskManagerState;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.exception.PrimusAMException;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorState;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.KafkaMessageType;
import com.bytedance.primus.api.records.KafkaTask;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskCommand;
import com.bytedance.primus.api.records.TaskCommandType;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.KafkaTaskPBImpl;
import com.bytedance.primus.api.records.impl.pb.TaskCommandPBImpl;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.apiserver.proto.DataProto.KafkaSourceSpec;
import com.bytedance.primus.apiserver.proto.DataProto.KafkaSourceSpec.Topic;
import com.bytedance.primus.apiserver.records.DataSourceSpec;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaTaskManager replies on Kafka offset as the checkpoint for error handling.
 */
public class KafkaTaskManager implements TaskManager {

  private final Logger LOG;
  private final AMContext context;
  private final String name;
  private final DataStreamSpec dataStreamSpec;
  private final long version;

  private final List<Task> tasks = new LinkedList<>();
  private final Map<Integer, Long> dataConsumptionTimeMap = new ConcurrentHashMap<>();

  public KafkaTaskManager(
      AMContext context,
      String name,
      DataStreamSpec dataStreamSpec,
      long version
  ) throws PrimusAMException {
    this.LOG = LoggerFactory.getLogger(KafkaTaskManager.class.getName() + "[" + name + "]");
    this.context = context;
    this.name = name;
    this.dataStreamSpec = dataStreamSpec;
    this.version = version;

    int taskId = 1;
    for (DataSourceSpec dataSourceSpec : dataStreamSpec.getDataSourceSpecs()) {
      KafkaSourceSpec kafkaSourceSpec = dataSourceSpec.getKafkaSourceSpec();
      resetOffset(kafkaSourceSpec);

      Topic topic = kafkaSourceSpec.getTopic();
      KafkaTask kafkaTask = new KafkaTaskPBImpl();
      kafkaTask.setTopic(topic.getTopic());
      kafkaTask.setConsumerGroup(topic.getConsumerGroup());
      kafkaTask.setConfig(topic.getConfigMap());
      kafkaTask.setKafkaMessageType(
          KafkaMessageType.valueOf(kafkaSourceSpec.getKafkaMessageType().name()));

      Task task = new TaskPBImpl();
      task.setGroup(name);
      task.setTaskId(taskId);
      task.setSourceId(dataSourceSpec.getSourceId());
      task.setSource(dataSourceSpec.getSource());
      task.setKafkaTask(kafkaTask);
      tasks.add(task);

      taskId += 1;
      dataConsumptionTimeMap.put(dataSourceSpec.getSourceId(), 0L);
    }
  }

  private boolean isKilling(ExecutorId executorId) {
    if (context.getSchedulerExecutorManager() == null) {
      return false;
    }
    return context
        .getSchedulerExecutorManager()
        .getRunningExecutorMap()
        .get(executorId)
        .getExecutorState() == SchedulerExecutorState.KILLING;
  }

  private void addRemoveTaskCommand(long taskId, List<TaskCommand> taskCommands) {
    Task task = new TaskPBImpl();
    task.setGroup(name);
    task.setTaskId(taskId);
    TaskCommand taskCommand = new TaskCommandPBImpl();
    taskCommand.setTaskCommandType(TaskCommandType.REMOVE);
    taskCommand.setTask(task);
    taskCommands.add(taskCommand);
  }

  @Override
  public List<TaskCommand> heartbeat(
      ExecutorId executorId,
      List<TaskStatus> taskStatuses,
      boolean removeTask,
      boolean needMoreTask
  ) {
    List<TaskCommand> taskCommands = new ArrayList<>();

    if (removeTask || isKilling(executorId)) {
      for (TaskStatus taskStatus : taskStatuses) {
        addRemoveTaskCommand(taskStatus.getTaskId(), taskCommands);
        LOG.info("send remove task[{}] request to executor[{}]",
            taskStatus.getTaskId(),
            executorId);
      }
    } else if (taskStatuses.isEmpty() && needMoreTask) {
      for (Task task : tasks) {
        TaskCommand taskCommand = new TaskCommandPBImpl();
        taskCommand.setTaskCommandType(TaskCommandType.ASSIGN);
        taskCommand.setTask(task);
        taskCommands.add(taskCommand);
        LOG.info("send assign task[{}] request to executor[{}]", task.getUid(), executorId);
      }
    }

    // Update dataConsumptionTimeMap
    taskStatuses.forEach(taskStatus ->
        dataConsumptionTimeMap.merge(
            taskStatus.getSourceId(),
            taskStatus.getDataConsumptionTime(),
            Math::max)
    );

    return taskCommands;
  }

  @Override
  public void unregister(ExecutorId executorId) {
  }

  @Override
  public List<TaskWrapper> getTasksForHistory() {
    return new ArrayList<>();
  }

  @Override
  public TaskManagerState getState() {
    return TaskManagerState.RUNNING;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public Map<Integer, String> getDataSourceReports() {
    return dataConsumptionTimeMap.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        e -> String.format("Current consuming timestamp: %d", e.getValue())
    ));
  }

  @Override
  public boolean isFailure() {
    return false;
  }

  @Override
  public boolean isSuccess() {
    return false;
  }

  @Override // TODO: Stop executor from consuming from Kafka
  public void suspend(int snapshotId) {
  }

  @Override // TODO: Stop executor from consuming from Kafka
  public SuspendStatusEnum getSuspendStatus() {
    return SuspendStatusEnum.FINISHED_SUCCESS;
  }

  @Override // TODO: Stop executor from consuming from Kafka
  public void resume() {
  }

  @Override // TODO: Stop executor from consuming from Kafka
  public boolean isSuspend() {
    return false;
  }

  @Override // TODO: Surface snapshot is not supported
  public int getSnapshotId() {
    return -1;
  }

  @Override // TODO: Surface snapshot is not supported
  public boolean takeSnapshot(String savepointDir) {
    return true;
  }

  private void resetOffset(KafkaSourceSpec kafkaSourceSpec) throws PrimusAMException {
    Properties props = new Properties();
    String topic = kafkaSourceSpec.getTopic().getTopic();
    String groupId = kafkaSourceSpec.getTopic().getConsumerGroup();
    props.setProperty("topic", topic);
    props.setProperty("group.id", groupId);
    LOG.info("kafka topic is " + topic);
    LOG.info("kafka group.id is " + groupId);
    props.putAll(kafkaSourceSpec.getTopic().getConfigMap());
    KafkaConsumer consumer = new KafkaConsumer<>(props);
    try {
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
      List<TopicPartition> partitions = partitionInfos.stream()
          .map(p -> new TopicPartition(topic, p.partition())).collect(Collectors.toList());
      for (TopicPartition tp : partitions) {
        LOG.info("consumer partition:" + tp);
      }
      switch (kafkaSourceSpec.getTopic().getKafkaStartUpMode()) {
        case GROUP_OFFSETS:
          LOG.info("start up mode is GROUP_OFFSETS");
          break;
        case EARLIEST:
          LOG.info("start up mode is EARLIEST");
          Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(partitions);
          Map<TopicPartition, OffsetAndMetadata> beginOffsetsAndMetadata =
              beginOffsets.entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          kv -> kv.getKey(),
                          kv -> new OffsetAndMetadata(kv.getValue())));
          consumer.commitSync(beginOffsetsAndMetadata);
          for (Map.Entry entry : beginOffsetsAndMetadata.entrySet()) {
            LOG.info("partition[" + entry.getKey() + "] offset is set to " + entry.getValue());
          }
          break;
        case LATEST:
          LOG.info("start up mode is LATEST");
          Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
          Map<TopicPartition, OffsetAndMetadata> endOffsetsAndMetadata =
              endOffsets.entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          kv -> kv.getKey(),
                          kv -> new OffsetAndMetadata(kv.getValue())));
          consumer.commitSync(endOffsetsAndMetadata);
          for (Map.Entry entry : endOffsetsAndMetadata.entrySet()) {
            LOG.info("partition[" + entry.getKey() + "] offset is set to " + entry.getValue());
          }
          break;
        case TIMESTAMP:
          long timestamp = kafkaSourceSpec.getTopic().getStartUpTimestamp();
          LOG.info("start up mode is TIMESTAMP, timestamp is " + timestamp);
          for (TopicPartition topicPartition : partitions) {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
            if (offsetAndMetadata == null) {
              Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
              timestampsToSearch.put(topicPartition, timestamp);
              Map<TopicPartition, OffsetAndTimestamp> timestampOffsets = consumer
                  .offsetsForTimes(timestampsToSearch);
              Map<TopicPartition, OffsetAndMetadata> timestampOffsetsAndMetadata =
                  timestampOffsets.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              kv -> kv.getKey(),
                              kv -> new OffsetAndMetadata(kv.getValue().offset())));
              consumer.commitSync(timestampOffsetsAndMetadata);
              for (Map.Entry entry : timestampOffsetsAndMetadata.entrySet()) {
                LOG.info(
                    "partition[" + entry.getKey() + "] offset is set to " + entry.getValue());
              }
            }
          }
          break;
        default:
          throw new PrimusAMException("Unsupported kafka start up mode: " +
              kafkaSourceSpec.getTopic().getKafkaStartUpMode());
      }
    } finally {
      consumer.close();
    }
  }

  @Override
  public void stop() {
  }

  @Override
  public void succeed() {
  }

  @Override
  public DataStreamSpec getDataStreamSpec() {
    return dataStreamSpec;
  }

  @Override
  public long getVersion() {
    return version;
  }
}
