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

package com.bytedance.primus.executor.task.kafka;

import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.task.TaskRemovedEvent;
import com.bytedance.primus.executor.task.TaskRunner;
import com.bytedance.primus.executor.task.WorkerFeeder;
import com.bytedance.primus.io.datasource.kafka.KafkaJsonMessageBuilder;
import com.bytedance.primus.io.datasource.kafka.KafkaRawMessageBuilder;
import com.bytedance.primus.io.messagebuilder.MessageBuilder;
import com.bytedance.primus.utils.PrimusConstants;
import java.nio.BufferOverflowException;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTaskRunner implements TaskRunner {

  private static final Duration CONSUMER_POLL_TIMEOUT_DURATION = Duration.ofMillis(1);
  private static final int FEED_FAILED_INTERVAL_MS = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTaskRunner.class);

  private ExecutorContext context;
  private String executorId;
  private KafkaConsumer<byte[], byte[]> consumer;
  private WorkerFeeder workerFeeder;
  private Task task;
  private volatile TaskStatus taskStatus;
  private String topic;
  private volatile boolean isStopped;
  private Thread feedThread;
  private MessageBuilder messageBuilder;

  public KafkaTaskRunner(Task task, ExecutorContext context, WorkerFeeder workerFeeder) {
    this.context = context;
    this.executorId = context.getExecutorId().toString();
    this.task = task;
    this.workerFeeder = workerFeeder;
    this.taskStatus = new TaskStatusPBImpl();
    this.taskStatus.setGroup(task.getGroup());
    this.taskStatus.setTaskId(task.getTaskId());
    this.taskStatus.setSourceId(task.getSourceId());
    this.taskStatus.setProgress(1);
    this.taskStatus.setTaskState(TaskState.RUNNING);
    this.taskStatus.setCheckpoint(task.getCheckpoint());
    this.taskStatus.setNumAttempt(task.getNumAttempt());
    this.taskStatus.setLastAssignTime(new Date().getTime());
    LOG.info("Start task, " + taskStatus);
    this.topic = task.getKafkaTask().getTopic();
    this.isStopped = false;
    this.feedThread = new FeedThread();
    this.feedThread.setDaemon(true);

    Properties props = new Properties();
    props.setProperty("group.id", task.getKafkaTask().getConsumerGroup());
    props.putAll(task.getKafkaTask().getConfig());
    this.consumer = new KafkaConsumer<>(props);
    int messageBufferSize =
        context.getPrimusConf().getPrimusConf().getInputManager().getMessageBufferSize();
    if (messageBufferSize <= 0) {
      messageBufferSize = PrimusConstants.DEFAULT_MESSAGE_BUFFER_SIZE;
    }
    switch (task.getKafkaTask().getKafkaMessageType()) {
      case JSON:
        messageBuilder = new KafkaJsonMessageBuilder(messageBufferSize);
        break;
      case PB:
      default:
        messageBuilder = new KafkaRawMessageBuilder(messageBufferSize);
        break;
    }
  }

  @Override
  public void init() {
  }

  @Override
  public Task getTask() {
    return task;
  }

  @Override
  public TaskStatus getTaskStatus() {
    return taskStatus;
  }

  @Override
  public void startTaskRunner() {
    feedThread.start();
  }

  @Override
  public void stopTaskRunner() {
    isStopped = true;
    feedThread.interrupt();
    context.getTaskRunnerManager().getDispatcher().getEventHandler()
        .handle(new TaskRemovedEvent(task.getUid(), context.getExecutorId()));
  }

  private class FeedThread extends Thread {

    FeedThread() {
      super("FeedThread of Task[" + task.getUid() + "]");
    }

    @Override
    public void run() {
      consumer.subscribe(Collections.singletonList(topic));
      PrimusMetrics.TimerMetric latency;
      boolean skipping = context.getPrimusConf().getInputManager().getSkipRecords();
      out:
      while (!isStopped) {
        PrimusMetrics.TimerMetric pollLatency =
            PrimusMetrics.getTimerContextWithOptionalPrefix(
                PrimusMetrics.prefixWithSingleTag(
                    "executor.task_runner.kafka.latency", "executor_id", executorId));
        ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_TIMEOUT_DURATION);
        pollLatency.stop();

        try {
          for (ConsumerRecord<byte[], byte[]> record : records) {
            if (!isStopped) {
              messageBuilder.add(record.key(), record.value());
              PrimusMetrics.emitCounterWithOptionalPrefix(
                  "executor.task_runner.file.feed_records{executor_id=" + executorId + "}",
                  1);
              if (messageBuilder.needFlush() || skipping) {
                latency =
                    PrimusMetrics.getTimerContextWithOptionalPrefix(
                        PrimusMetrics.prefixWithSingleTag(
                            "executor.task_runner.feed.latency", "executor_id", executorId));
                workerFeeder
                    .feedSuccess(messageBuilder.build(), 0, messageBuilder.size(), skipping);
                latency.stop();
                PrimusMetrics.emitStoreWithOptionalPrefix(
                    PrimusMetrics.prefixWithSingleTag(
                        "executor.worker_feeder.kafka.feed_bytes", "executor_id", executorId),
                    messageBuilder.size());
                messageBuilder.reset();
              }
              taskStatus.setDataConsumptionTime(record.timestamp());
            } else {
              break out;
            }
          }

          if (messageBuilder.size() > 0) {
            workerFeeder.feedSuccess(messageBuilder.build(), 0, messageBuilder.size(), true);
            PrimusMetrics.emitStoreWithOptionalPrefix(
                PrimusMetrics.prefixWithSingleTag(
                    "executor.worker_feeder.file.feed_bytes", "executor_id", executorId),
                messageBuilder.size());
            messageBuilder.reset();
          }
        } catch (BufferOverflowException e) {
          LOG.warn("Failed to write message to buffer", e);
          PrimusMetrics.emitCounterWithOptionalPrefix(
              PrimusMetrics.prefixWithSingleTag(
                  "executor.task_runner.drop_records", "executor_id", executorId),
              messageBuilder.getMessageNum());
          messageBuilder.reset();
        } catch (Exception e) {
          LOG.warn("Feed error in feed thread", e);
          try {
            Thread.sleep(FEED_FAILED_INTERVAL_MS);
          } catch (InterruptedException e2) {
            // ignore
          }
        }
      }
    }
  }
}
