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
import com.bytedance.primus.am.ApplicationMasterEvent;
import com.bytedance.primus.am.ApplicationMasterEventType;
import com.bytedance.primus.api.records.FileTask;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.impl.pb.FileTaskPBImpl;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.io.datasource.file.FileDataSource;
import com.bytedance.primus.io.datasource.file.models.BaseInput;
import com.bytedance.primus.io.datasource.file.models.BaseSplit;
import com.bytedance.primus.io.datasource.file.models.PrimusInput;
import com.bytedance.primus.io.datasource.file.models.PrimusSplit;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTaskBuilder {

  private static final int IO_RETRY_TIMES = 3;
  private static final int MIN_NUM_BUILD_TASK_THREADS = 1;
  private static final int MAX_NUM_BUILD_TASK_THREADS = 50;

  private Logger LOG;

  private final AMContext context;
  private final FileSystem fileSystem;

  private String name;
  private TaskStore taskStore;
  private Task lastSavedTask;
  private FileScanner fileScanner;

  private long currentTaskId;
  private volatile boolean isFinished;
  private volatile boolean isStopped = false;

  private SplitterThread splitterThread;
  private BuilderThread builderThread;
  private static final long POLL_PERIOD_MINUTES = 5;

  // SplitterThread puts splits to queue and BuilderThread polls splits from queue.
  BlockingQueue<Future<List<BaseSplit>>> splitsBlockingQueue;

  public FileTaskBuilder(AMContext context, String name, DataStreamSpec dataStreamSpec,
      TaskStore taskStore) throws IOException {
    this.LOG = LoggerFactory.getLogger(FileTaskBuilder.class.getName() + "[" + name + "]");
    this.context = context;
    this.fileSystem = context.getHadoopFileSystem();

    this.name = name;
    this.taskStore = taskStore;
    lastSavedTask = taskStore.getLastSavedTask();
    fileScanner = new FileScanner(context, name, dataStreamSpec);
    currentTaskId = (lastSavedTask != null ? lastSavedTask.getTaskId() : 0);
    isFinished = false;

    int numBuildTaskThreads = Math.max(MIN_NUM_BUILD_TASK_THREADS, context.getPrimusConf()
        .getInputManager().getNumBuildTaskThreads());
    if (numBuildTaskThreads > MAX_NUM_BUILD_TASK_THREADS) {
      LOG.warn("User's numBuildTaskThreads is too large, use max default number "
          + MAX_NUM_BUILD_TASK_THREADS);
      numBuildTaskThreads = MAX_NUM_BUILD_TASK_THREADS;
    }
    splitsBlockingQueue = new LinkedBlockingQueue<>(numBuildTaskThreads * 10000); // avoid OOM
    splitterThread = new SplitterThread(numBuildTaskThreads);
    splitterThread.start();
    builderThread = new BuilderThread();
    builderThread.start();
  }

  public boolean isFinished() {
    return isFinished;
  }

  private void failedApp(String diag, ApplicationMasterEventType eventType, int exitCode) {
    LOG.error(diag);
    context.getDispatcher().getEventHandler()
        .handle(new ApplicationMasterEvent(context, eventType, diag, exitCode));
  }

  public List<BaseSplit> getFileSplits(FileSystem fileSystem, BaseInput input) {
    TimerMetric latency = PrimusMetrics.getTimerContextWithAppIdTag(
        "am.taskbuilder.splitter.latency", new HashMap<>());
    List<BaseSplit> result = new LinkedList<>();
    int ioExceptions = 0;
    while (true) {
      try {
        if (input instanceof PrimusInput) {
          PrimusInput primusInput = (PrimusInput) input;
          FileDataSource source = FileDataSource.load(primusInput.getSpec());
          result = new ArrayList<>(source.scanPattern(fileSystem, primusInput));
        }
        break;
      } catch (AccessControlException e) {
        // GDPR
        String diag = "Failed to get splits for input [" + input + "], fail job because of " + e;
        failedApp(diag, ApplicationMasterEventType.FAIL_APP, ApplicationExitCode.GDPR.getValue());
      } catch (IllegalArgumentException e) {
        // Wrong FS
        String diag = "Failed to get splits for input [" + input + "], fail job because of " + e;
        failedApp(diag, ApplicationMasterEventType.FAIL_APP,
            ApplicationExitCode.WRONG_FS.getValue());
      } catch (NoSuchFileException e) {
        LOG.warn("Skip input " + input, e);
        break;
      } catch (IOException e) {
        LOG.warn("Failed to get file splits for " + input + ", retry", e);
        ioExceptions += 1;
        if (ioExceptions > IO_RETRY_TIMES) {
          LOG.warn("Failed to get file splits for " + input + ", skip it", e);
          break;
        }
      } catch (Exception e) {
        LOG.error("Failed to get file splits for " + input + ", throw exception", e);
        throw e;
      }
    }
    latency.stop();
    return result;
  }

  public List<BaseSplit> getFileSplits(FileSystem fileSystem, List<BaseInput> inputs) {
    List<BaseSplit> primusSplits = new LinkedList<>();
    String key = null;
    for (BaseInput input : inputs) {
      // check they have the same key
      if (key == null) {
        key = input.getBatchKey();
      } else {
        assert key.equals(input.getBatchKey());
      }
      primusSplits.addAll(getFileSplits(fileSystem, input));
    }

    return primusSplits;
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

  class SplitterThread extends Thread {

    private int numThreads;

    public SplitterThread(int numThreads) {
      super(SplitterThread.class.getName() + "[" + name + "]");
      setDaemon(true);
      this.numThreads = numThreads;
    }

    @Override
    public void run() {
      ExecutorService pool = Executors
          .newFixedThreadPool(numThreads, new ThreadFactoryBuilder().setDaemon(true).build());
      while (!isStopped) {
        try {
          // blocking until available
          List<BaseInput> inputs = fileScanner.getInputQueue().take();
          Future<List<BaseSplit>> future = pool
              .submit(() -> getFileSplits(fileSystem, inputs));
          splitsBlockingQueue.put(future);  // blocking if no capacity, for avoiding OOM
        } catch (InterruptedException interruptedException) {
          LOG.info("Ignore interrupted exception and continue to get inputs");
        } catch (Exception e) {
          failedApp("Failed to get inputs from file scanner and split " + e,
              ApplicationMasterEventType.FAIL_APP,
              ApplicationExitCode.BUILD_TASK_FAILED.getValue());
          break;
        }
      }
      pool.shutdownNow();
    }
  }

  class BuilderThread extends Thread {

    private static final int BUFFERED_ROUNDS = 3;

    public BuilderThread() {
      super(BuilderThread.class.getName() + "[" + name + "]");
      setDaemon(true);
    }

    @Override
    public void run() {
      Future<List<BaseSplit>> futureSplits = takeFutureFromQueue(splitsBlockingQueue);
      if (futureSplits == null && !isStopped) {
        failedApp("Failed to get the 1st splits from splitter thread",
            ApplicationMasterEventType.FAIL_APP,
            ApplicationExitCode.BUILD_TASK_FAILED.getValue());
        return;
      }

      List<Task> bufferedTasks = new LinkedList<>();
      int bufferedRounds = 0;
      while (!isStopped) {
        if (futureSplits != null) {
          List<BaseSplit> splits;
          try {
            splits = futureSplits.get();
          } catch (InterruptedException interruptedException) {
            LOG.info("Ignore interrupted exception and continue to get splits");
            continue;
          } catch (Exception e) {
            String diag = "Failed to get future splits, fail job because of " + e;
            failedApp(diag, ApplicationMasterEventType.FAIL_APP,
                ApplicationExitCode.BUILD_TASK_FAILED.getValue());
            return;
          }
          List<Task> tasks = buildTask(splits);
          if (tasks != null && !tasks.isEmpty()) {
            logTaskEvent(tasks);
            bufferedTasks.addAll(tasks);
            bufferedRounds += 1;
          }
          if (bufferedRounds > BUFFERED_ROUNDS && !bufferedTasks.isEmpty()) {
            taskStore.addNewTasks(bufferedTasks);
            // clear buffered tasks
            bufferedTasks = new LinkedList<>();
            bufferedRounds = 0;
          }
          // non-blocking
          futureSplits = splitsBlockingQueue.poll();
        } else {
          if (!bufferedTasks.isEmpty()) {
            taskStore.addNewTasks(bufferedTasks);
            // clear buffered tasks
            bufferedTasks = new LinkedList<>();
            bufferedRounds = 0;
          }
          if (fileScanner.isFinished()) {
            isFinished = true;
            LOG.info(
                "Finish building tasks for exist inputs, waiting for newly generating inputs");
          }
          try {
            futureSplits = splitsBlockingQueue.poll(POLL_PERIOD_MINUTES, TimeUnit.MINUTES);
          } catch (InterruptedException e) {
            futureSplits = null;
            LOG.warn("splitsBlockingQueue poll interrupted");
            break;
          }
        }
      }
      if (!bufferedTasks.isEmpty()) {
        taskStore.addNewTasks(bufferedTasks);
      }
      LOG.info("Finished building all tasks, total task number: " + taskStore.getTotalTaskNum());
      context.getTimelineLogger()
          .logEvent(PRIMUS_TASKS_TOTAL_COUNT.name(), Long.toString(taskStore.getTotalTaskNum()));
    }
  }

  private Future<List<BaseSplit>> takeFutureFromQueue(
      BlockingQueue<Future<List<BaseSplit>>> blockingQueue) {
    try {
      // blocking until available
      return blockingQueue.take();
    } catch (InterruptedException e) {
      LOG.warn("Get splits from splitter thread caught interrupted exception");
      return null;
    }
  }

  private void logTaskEvent(List<Task> tasks) {
    for (Task task : tasks) {
      context.getTimelineLogger().logEvent(PRIMUS_TASK_INFO_DETAILED.name(), task.toString());
      if (task.getFileTask() != null && task.getFileTask().getLength() > 0) {
        context.getTimelineLogger()
            .logEvent(PRIMUS_TASK_INFO_FILE_SIZE.name(),
                Long.toString(task.getFileTask().getLength()));
      }
    }
  }

  public void stop() {
    isStopped = true;
    builderThread.interrupt();
    splitterThread.interrupt();
    try {
      builderThread.join();
      LOG.info("builder thread stopped");
      splitterThread.join();
      LOG.info("splitter thread stopped");
      fileScanner.stop();
      LOG.info("file scanner stopped");
    } catch (InterruptedException e) {
      LOG.warn("Stopping caught interrupted exception", e);
    }
  }
}
