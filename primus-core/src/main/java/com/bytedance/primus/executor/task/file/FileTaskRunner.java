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

package com.bytedance.primus.executor.task.file;

import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_PERFORMANCE_EVENT;

import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.InputTypeCase;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.task.TaskRemovedEvent;
import com.bytedance.primus.executor.task.TaskRunner;
import com.bytedance.primus.executor.task.WorkerFeeder;
import com.bytedance.primus.executor.timeline.TimeLineConfigHelper;
import com.bytedance.primus.io.datasource.file.impl.raw.RawInputFormat;
import com.bytedance.primus.io.messagebuilder.MessageBuilder;
import com.bytedance.primus.utils.PrimusConstants;
import com.bytedance.primus.utils.timeline.TimelineLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FileTaskRunner.class);
  private static final int FEED_FAILED_INTERVAL_MS = 1000;

  protected Task task;
  protected ExecutorContext context;
  protected JobConf jobConf;
  protected volatile TaskStatus taskStatus;
  protected volatile TaskCheckpoint taskCheckpoint;
  protected Reporter taskReporter;

  private volatile boolean isStopped;
  private Thread feedThread;
  private WorkerFeeder workerFeeder;
  private int maxAllowedIOException;
  private ExecutorService feedThreadPool;

  public FileTaskRunner(Task task, ExecutorContext context, WorkerFeeder workerFeeder) {
    this.task = task;
    this.context = context;
    this.jobConf = new JobConf(context.getPrimusConf().getHadoopConf());
    this.taskStatus = new TaskStatusPBImpl();
    this.taskStatus.setGroup(task.getGroup());
    this.taskStatus.setTaskId(task.getTaskId());
    this.taskStatus.setSourceId(task.getSourceId());
    this.taskStatus.setProgress(0);
    this.taskStatus.setTaskState(TaskState.RUNNING);
    this.taskStatus.setCheckpoint(task.getCheckpoint());
    this.taskStatus.setNumAttempt(task.getNumAttempt());
    this.taskStatus.setLastAssignTime(new Date().getTime());
    LOG.info("Start task, " + taskStatus);
    taskCheckpoint = new TaskCheckpoint();
    this.taskReporter = new TaskReporter();
    this.isStopped = false;
    this.feedThread =
        new FeedThread(context.getTimelineLogger(), context.getExecutorId().toString());
    this.feedThread.setDaemon(true);
    this.workerFeeder = workerFeeder;
    this.maxAllowedIOException = context.getPrimusConf().getInputManager()
        .getMaxAllowedIoException();
    if (maxAllowedIOException <= 0) {
      maxAllowedIOException = PrimusConstants.DEFAULT_MAX_ALLOWED_IO_EXCEPTION;
    }
  }

  @Override
  public void init() throws Exception {
  }

  public abstract RecordReader<Object, Object> getRecordReader();

  public abstract RecordReader<Object, Object> createRecordReader() throws Exception;

  public abstract MessageBuilder getMessageBuilder();

  public abstract long getLength();

  public abstract int getRewindSkipNum();

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
    try {
      feedThread.start();
    } catch (Throwable e) {
      taskStatus.setTaskState(TaskState.FAILED);
      LOG.warn("Task[" + task.getUid() + "] runner start error, " + taskStatus, e);
    }
  }

  @Override
  public void stopTaskRunner() {
    isStopped = true;
    feedThread.interrupt();
    LOG.info("Task[" + task.getUid() + "] feeder thread exit, isAlive: " + feedThread.isAlive());
    taskCheckpoint.setAccurate(true);
    taskStatus.setCheckpoint(taskCheckpoint.getCheckpoint());
    context.getTaskRunnerManager().getDispatcher().getEventHandler()
        .handle(new TaskRemovedEvent(task.getUid(), context.getExecutorId()));
  }

  private class FeedThread extends Thread {

    public static final String EXECUTOR_TASK_RUNNER_TOTAL_LATENCY = "executor.task_runner.total.latency";
    public static final String EXECUTOR_TASK_RUNNER_READ_LATENCY = "executor.task_runner.read.latency";
    public static final String EXECUTOR_TASK_RUNNER_CHECKPOINT_LATENCY = "executor.task_runner.checkpoint.latency";
    public static final String EXECUTOR_TASK_RUNNER_BUILD_LATENCY = "executor.task_runner.build.latency";
    public static final String EXECUTOR_TASK_RUNNER_FEED_LATENCY = "executor.task_runner.feed.latency";

    private TimelineLogger timelineLogger;
    private String executorId;
    private RecordReader<Object, Object> reader;

    FeedThread(TimelineLogger timelineLogger, String executorId) {
      super("FeedThread of Task[" + task.getUid() + "]");
      this.executorId = executorId;
      this.timelineLogger = timelineLogger;
    }

    public void waitUntilFeedSuccess(byte b[], int start, int length, boolean flush)
        throws Exception {
      boolean closed = false;
      Future<?> future = feedThreadPool.submit(() -> {
        try {
          workerFeeder.feedSuccess(b, start, length, flush);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      try {
        future.get(10, TimeUnit.MINUTES);
      } catch (TimeoutException e) {
        LOG.info("Feed task [{}] timeout, close reader!", task.getUid());
        reader.close();
        closed = true;
      }
      if (closed) {
        LOG.info("Continue feedTask after closed reader of task [{}]", task.getUid());
        future.get();
        reader = createRecordReader();
        LOG.info("Feed task [{}] finished, reopen reader!", task.getUid());
      }
    }

    @Override
    public void run() {
      try {
        init();
      } catch (Exception e) {
        taskStatus.setTaskState(TaskState.FAILED);
        LOG.info("Task[" + taskStatus.getTaskId() + "] fail", e);
        return;
      }
      int taskFeedMetricBatchSize = TimeLineConfigHelper
          .getMaxTaskFeedMetricBatchSize(timelineLogger.getTimelineConfig());
      MetricToTimeLineEventBridge metricToTimeLineEventBridge = new MetricToTimeLineEventBridge(
          timelineLogger, PRIMUS_TASK_PERFORMANCE_EVENT.name(), taskFeedMetricBatchSize);
      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("TaskFeedMonitorThread-" + task.getUid()).build();
      reader = getRecordReader();
      Object key = reader.createKey();
      Object value = reader.createValue();
      MessageBuilder messageBuilder = getMessageBuilder();
      boolean succeed = false;
      int rewindSkipNum = getRewindSkipNum();
      boolean skipping = context.getPrimusConf().getInputManager().getSkipRecords();
      PrimusMetrics.TimerMetric latency;
      feedThreadPool = Executors.newSingleThreadExecutor(threadFactory);
      try {
        taskCheckpoint.parseCheckpoint(task.getCheckpoint());
        if (taskCheckpoint.getAccurate()) {
          rewindSkipNum = 0;
        }
        taskCheckpoint.setAccurate(false);
        int ioExceptionNums = 0;
        while (!isStopped) {
          PrimusMetrics.TimerMetric totalLatency =
              PrimusMetrics.getTimerContextWithOptionalPrefix(
                  PrimusMetrics.prefixWithSingleTag(
                      EXECUTOR_TASK_RUNNER_TOTAL_LATENCY, "executor_id", executorId));
          try {
            latency = PrimusMetrics
                .getTimerContextWithOptionalPrefix(
                    PrimusMetrics.prefixWithSingleTag(
                        EXECUTOR_TASK_RUNNER_READ_LATENCY, "executor_id", executorId));
            boolean hasNext = reader.next(key, value);
            metricToTimeLineEventBridge.record(EXECUTOR_TASK_RUNNER_READ_LATENCY, latency.stop());
            latency = PrimusMetrics
                .getTimerContextWithOptionalPrefix(
                    PrimusMetrics.prefixWithSingleTag(
                        EXECUTOR_TASK_RUNNER_CHECKPOINT_LATENCY, "executor_id", executorId));
            if (!hasNext) {
              succeed = true;
              break;
            }
            taskStatus.setProgress(reader.getProgress());
            if (reader.getPos() < taskCheckpoint.getPos()) {
              continue;
            }
            taskCheckpoint.setPos(reader.getPos());
            taskStatus.setCheckpoint(taskCheckpoint.getCheckpoint());
            if (task.getNumAttempt() > 1 && (rewindSkipNum--) > 0) {
              PrimusMetrics
                  .emitCounterWithOptionalPrefix(
                      PrimusMetrics.prefixWithSingleTag(
                          "executor.task_runner.file.skip_records", "executor_id", executorId), 1);
              continue;
            }
            metricToTimeLineEventBridge
                .record(EXECUTOR_TASK_RUNNER_CHECKPOINT_LATENCY, latency.stop());
            latency =
                PrimusMetrics.getTimerContextWithOptionalPrefix(
                    PrimusMetrics.prefixWithSingleTag(
                        EXECUTOR_TASK_RUNNER_BUILD_LATENCY, "executor_id", executorId));
            messageBuilder.add(key, value);
            metricToTimeLineEventBridge.record(EXECUTOR_TASK_RUNNER_BUILD_LATENCY, latency.stop());

            PrimusMetrics
                .emitCounterWithOptionalPrefix(
                    PrimusMetrics.prefixWithSingleTag(
                        "executor.task_runner.file.feed_records", "executor_id", executorId), 1);

            if (messageBuilder.needFlush() || skipping) {
              latency =
                  PrimusMetrics.getTimerContextWithOptionalPrefix(
                      PrimusMetrics.prefixWithSingleTag(
                          EXECUTOR_TASK_RUNNER_FEED_LATENCY, "executor_id", executorId));
              waitUntilFeedSuccess(messageBuilder.build(), 0, messageBuilder.size(), skipping);
              metricToTimeLineEventBridge.record(EXECUTOR_TASK_RUNNER_FEED_LATENCY, latency.stop());
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
          } catch (FileNotFoundException e) {
            LOG.warn("File not found", e);
            break;
          } catch (BlockMissingException e) {
            LOG.warn("Block missing in HDFS", e);
            break;
          } catch (IOException e) {
            if ((++ioExceptionNums) >= maxAllowedIOException) {
              LOG.warn(" Exceed max allowed error times in feed thread.", e);
              break;
            } else {
              LOG.warn(ioExceptionNums + " times feed error in feed thread. Allowed times: "
                  + maxAllowedIOException, e);
            }
            try {
              Thread.sleep(FEED_FAILED_INTERVAL_MS);
            } catch (InterruptedException e2) {
              // ignore
            }
          }
          metricToTimeLineEventBridge
              .record(EXECUTOR_TASK_RUNNER_TOTAL_LATENCY, totalLatency.stop());
          metricToTimeLineEventBridge.flush();
        }
      } catch (InterruptedException e) {
        // ignore
      } catch (Throwable e) {
        LOG.warn("Something wrong in feed thread", e);
        throw new RuntimeException("Something wrong in feed thread", e);
      } finally {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.info("Reader close failed", e);
        }
        if (succeed) {
          if (messageBuilder.size() > 0) {
            LOG.info("Feeding remain message bytes to worker");
            try {
              latency =
                  PrimusMetrics.getTimerContextWithOptionalPrefix(
                      PrimusMetrics.prefixWithSingleTag(
                          EXECUTOR_TASK_RUNNER_FEED_LATENCY, "executor_id", executorId));
              waitUntilFeedSuccess(messageBuilder.build(), 0, messageBuilder.size(), true);
              metricToTimeLineEventBridge.record(EXECUTOR_TASK_RUNNER_FEED_LATENCY, latency.stop());
            } catch (Exception e) {
              LOG.warn("Feeding remain message bytes to worker catch exception, ignore", e);
            }
          }
          taskStatus.setProgress(1);
          taskStatus.setTaskState(TaskState.SUCCEEDED);
          taskCheckpoint.setAccurate(true);
          taskStatus.setCheckpoint(taskCheckpoint.getCheckpoint());
          LOG.info("Task[" + task.getUid() + "] success");
        } else if (isStopped) {
          taskStatus.setTaskState(TaskState.RUNNING);
          taskCheckpoint.setAccurate(true);
          taskStatus.setCheckpoint(taskCheckpoint.getCheckpoint());
        } else {
          taskStatus.setTaskState(TaskState.FAILED);
          LOG.info("Task[" + task.getUid() + "] fail");
        }
        LOG.info("Current taskStatus, " + taskStatus);
        metricToTimeLineEventBridge.close();
        feedThreadPool.shutdownNow();
      }
    }
  }

  public InputFormat createInputFormat(JobConf jobConf, InputTypeCase inputType) {
    InputFormat inputFormat;
    try {
      switch (inputType) {
        case RAW_INPUT:
          inputFormat = (InputFormat<Writable, Writable>) Class
              .forName(RawInputFormat.class.getCanonicalName()).newInstance();
          break;
        case TEXT_INPUT:
        default:
          inputFormat = (InputFormat<Writable, Writable>) Class
              .forName(TextInputFormat.class.getCanonicalName()).newInstance();
          break;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create input format", e);
    }
    return inputFormat;
  }

  protected int getMessageBufferSize() {
    int messageBufferSize = context.getPrimusConf().getInputManager().getMessageBufferSize();
    if (messageBufferSize <= 0) {
      messageBufferSize = PrimusConstants.DEFAULT_MESSAGE_BUFFER_SIZE;
    }
    return messageBufferSize;
  }
}
