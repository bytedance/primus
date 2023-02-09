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

package com.bytedance.primus.am.datastream.file;

import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASKS_TOTAL_COUNT;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_INFO_DETAILED;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_INFO_FILE_SIZE;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.api.records.FileTask;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.impl.pb.FileTaskPBImpl;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.io.datasource.file.models.BaseSplit;
import com.bytedance.primus.io.datasource.file.models.PrimusSplit;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTaskBuilder {

  private static final long POLL_PERIOD_MINUTES = 5;
  private static final int MIN_NUM_BUILD_TASK_THREADS = 1;
  private static final int MAX_NUM_BUILD_TASK_THREADS = 50;

  private final Logger LOG;

  private final AMContext context;
  @Getter
  private final String name;
  private final TaskStore taskStore;
  private final Task lastSavedTask;

  private long currentTaskId;
  @Getter
  private volatile boolean isFinished = false;
  @Getter
  private volatile boolean isStopped = false;

  @Getter
  private final FileScanner fileScanner;
  private final FileSplitter splitter;
  private final BuilderThread builderThread;

  public FileTaskBuilder(
      AMContext context,
      String dataStreamName,
      DataStreamSpec dataStreamSpec,
      TaskStore taskStore
  ) {
    this.LOG = LoggerFactory.getLogger(
        FileTaskBuilder.class.getName() + "[" + dataStreamName + "]");

    this.context = context;
    this.name = dataStreamName;
    this.taskStore = taskStore;
    this.lastSavedTask = taskStore.getLastSavedTask();

    this.currentTaskId = (lastSavedTask != null ? lastSavedTask.getTaskId() : 0);
    this.isFinished = false;

    int numBuildTaskThreads = Math.max(
        MIN_NUM_BUILD_TASK_THREADS,
        context.getApplicationMeta().getPrimusConf().getInputManager().getNumBuildTaskThreads()
    );
    if (numBuildTaskThreads > MAX_NUM_BUILD_TASK_THREADS) {
      LOG.warn(
          "User's numBuildTaskThreads is too large, use max default number({})",
          MAX_NUM_BUILD_TASK_THREADS);
      numBuildTaskThreads = MAX_NUM_BUILD_TASK_THREADS;
    }

    fileScanner = new FileScanner(context, dataStreamName, dataStreamSpec);
    fileScanner.start();
    splitter = new FileSplitter(context, this, numBuildTaskThreads);
    splitter.start();
    builderThread = new BuilderThread();
    builderThread.start();
  }

  public void stop() {
    isStopped = true;
    builderThread.interrupt();
    splitter.interrupt();
    try {
      builderThread.join();
      LOG.info("builder thread stopped");
      splitter.join();
      LOG.info("splitter thread stopped");
      fileScanner.stop();
      LOG.info("file scanner stopped");
    } catch (InterruptedException e) {
      LOG.warn("Stopping caught interrupted exception", e);
    }
  }

  private void failApplication(String diag, int exitCode) {
    LOG.error(diag);
    context.emitFailApplicationEvent(diag, exitCode);
  }

  class BuilderThread extends Thread {

    private static final int BUFFERED_ROUNDS = 3;

    public BuilderThread() {
      super(BuilderThread.class.getName() + "[" + name + "]");
      setDaemon(true);
    }

    @Override
    public void run() {
      int bufferedRounds = 0;
      List<Task> bufferedTasks = new LinkedList<>();
      while (!isStopped) {
        // Try getting splits
        List<BaseSplit> splits = new LinkedList<>();
        try {
          Future<List<BaseSplit>> future = splitter.poll(POLL_PERIOD_MINUTES, TimeUnit.MINUTES);
          if (future == null) { // flush buffer if there is no split for a while
            taskStore.addNewTasks(bufferedTasks);
            bufferedRounds = 0;
            bufferedTasks.clear();
          } else {
            splits = future.get();
          }
        } catch (InterruptedException e) {
          LOG.warn("splitsBlockingQueue poll interrupted");
        } catch (ExecutionException e) {
          failApplication(
              "Failed to get future splits, fail job because of " + e.getMessage(),
              ApplicationExitCode.BUILD_TASK_FAILED.getValue());
        }

        // build and buffer tasks
        List<Task> tasks = buildTask(splits);
        if (!tasks.isEmpty()) {
          logTaskTimelineEvent(tasks);
          bufferedTasks.addAll(tasks);
          bufferedRounds += 1;

          if (bufferedRounds >= BUFFERED_ROUNDS) { // flush buffer
            taskStore.addNewTasks(bufferedTasks);
            bufferedRounds = 0;
            bufferedTasks.clear();
          }
        }

        // Finish check
        if (fileScanner.isFinished() &&
            fileScanner.isEmpty() &&
            splitter.isEmpty()
        ) {
          taskStore.addNewTasks(bufferedTasks);
          isFinished = true;

          LOG.info("Finished building all tasks, total tasks: " + taskStore.getTotalTaskNum());
          context.logTimelineEvent(
              PRIMUS_TASKS_TOTAL_COUNT.name(),
              Long.toString(taskStore.getTotalTaskNum())
          );

          return;
        }
      }
    }

    private List<Task> buildTask(List<BaseSplit> splits) {
      TimerMetric latency =
          PrimusMetrics.getTimerContextWithAppIdTag(
              "am.taskbuilder.builder.latency", new HashMap<>());

      // Building tasks, note the order matters
      List<Task> tasks = new LinkedList<>();
      for (BaseSplit split : splits) {
        if (lastSavedTask == null || !split.hasBeenBuilt(
            lastSavedTask.getFileTask().getBatchKey(),
            lastSavedTask.getSourceId(),
            lastSavedTask.getFileTask().getPath()
        )) {
          tasks.add(newTask(split, ++currentTaskId));
        }
      }

      if (!splits.isEmpty() && !tasks.isEmpty()) {
        LOG.info("Add tasks[" + tasks.get(0).getTaskId() + " - "
            + tasks.get(tasks.size() - 1).getTaskId()
            + "] for input[key: " + splits.get(0).getBatchKey()
            + ", source: " + splits.get(0).getSourceId()
            + ", spec: " + splits.get(0).getSpec()
            + ", path of a split: " + splits.get(0).getPath() + "]");
      }

      latency.stop();
      return tasks;
    }

    // TODO: Remove protobuf wrapper and move this function to PrimusSplit implementation.
    private Task newTask(BaseSplit split, long taskId) {
      Task task = new TaskPBImpl();
      if (split instanceof PrimusSplit) {
        PrimusSplit primusSplit = (PrimusSplit) split;
        FileTask fileTask = new FileTaskPBImpl(
            primusSplit.getPath(),
            primusSplit.getStart(),
            primusSplit.getLength(),
            primusSplit.getBatchKey(),
            primusSplit.getSpec()
        );

        task.setGroup(name);
        task.setTaskId(taskId);
        task.setSourceId(primusSplit.getSourceId());
        task.setSource(primusSplit.getSource());
        task.setFileTask(fileTask);
        task.setNumAttempt(0);
        task.setCheckpoint("");
      }
      return task;
    }

    private void logTaskTimelineEvent(List<Task> tasks) {
      for (Task task : tasks) {
        context.logTimelineEvent(
            PRIMUS_TASK_INFO_DETAILED.name(),
            task.toString());
        if (task.getFileTask() != null && task.getFileTask().getLength() > 0) {
          context.logTimelineEvent(
              PRIMUS_TASK_INFO_FILE_SIZE.name(),
              Long.toString(task.getFileTask().getLength()));
        }
      }
    }
  }
}
