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

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.controller.SuspendStatusEnum;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.TaskManagerState;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.datastream.file.task.builder.FileTaskBuilder;
import com.bytedance.primus.am.datastream.file.task.store.TaskStatistics;
import com.bytedance.primus.am.datastream.file.task.store.TaskStore;
import com.bytedance.primus.am.datastream.file.task.store.filesystem.FileSystemTaskStore;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorState;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.FileTask;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskCommand;
import com.bytedance.primus.api.records.TaskCommandType;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskCommandPBImpl;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.apiserver.records.DataSourceSpec;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.util.FloatUtils;
import com.bytedance.primus.common.util.IntegerUtils;
import com.bytedance.primus.common.util.PrimusConstants;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.proto.PrimusInput.InputManager;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTaskManager implements TaskManager {

  private static final String DEFAULT_BATCH_KEY = "--"; // Smaller than YYYYMMDD and YYYYMMDDHH
  private static final int DEFAULT_GRACEFUL_SHUTDOWN_TIME_WAIT_SEC = 30 * 60;
  private static final int DEFAULT_GRACEFUL_SHUTDOWN_TIME_CHECK_SEC = 30;
  public static final int HISTORY_FAILURE_TASKS_LIMIT_COUNT = 10000;

  private final Logger LOG;
  private final AMContext context;
  private final String name;
  private final DataStreamSpec dataStreamSpec;
  private final Map<ExecutorId, Date> executorLatestLaunchTimes;

  private final TaskStore taskStore;
  private final FileTaskBuilder taskBuilder;

  private final int maxTaskNumPerWorker;
  private final int maxTaskAttempts;
  private final float taskSuccessPercent;
  private final float taskFailedPercent;

  private volatile boolean isSuspended = false;
  private volatile int snapshotId = 0;
  private volatile boolean isTimeoutChecking = false;

  private final Map<Integer, String> dataSourceBatchKeyMap;

  private final long version;
  private volatile boolean isStopped = false;
  private volatile boolean isSucceed = false;

  public FileTaskManager(
      AMContext context,
      String name,
      DataStreamSpec dataStreamSpec,
      String savepointDir,
      long version
  ) throws IOException {
    this.version = version;
    this.LOG = LoggerFactory.getLogger(FileTaskManager.class.getName() + "[" + name + "]");
    this.context = context;
    this.name = name;
    this.dataStreamSpec = dataStreamSpec;
    this.executorLatestLaunchTimes = new ConcurrentHashMap<>();
    this.taskStore = new FileSystemTaskStore(context, name, savepointDir);
    this.taskBuilder = new FileTaskBuilder(context, name, dataStreamSpec, taskStore);

    InputManager inputManager = context.getApplicationMeta().getPrimusConf().getInputManager();
    maxTaskNumPerWorker = IntegerUtils.ensurePositiveOrDefault(
        inputManager.getMaxTaskNumPerWorker(),
        PrimusConstants.DEFAULT_MAX_TASK_NUM_PER_WORKER
    );
    maxTaskAttempts = IntegerUtils.ensurePositiveOrDefault(
        inputManager.getMaxTaskAttempts(),
        PrimusConstants.DEFAULT_MAX_TASK_ATTEMPTS
    );
    taskSuccessPercent = FloatUtils.ensurePositiveOrDefault(
        inputManager.getFileConfig().getStopPolicy().getTaskSuccessPercent(),
        PrimusConstants.DEFAULT_SUCCESS_PERCENT
    );
    taskFailedPercent = FloatUtils.ensurePositiveOrDefault(
        inputManager.getFileConfig().getStopPolicy().getTaskFailedPercent(),
        PrimusConstants.DEFAULT_FAILED_PERCENT
    );

    dataSourceBatchKeyMap = new ConcurrentHashMap<>();
    for (DataSourceSpec dataSourceSpec : dataStreamSpec.getDataSourceSpecs()) {
      dataSourceBatchKeyMap.put(dataSourceSpec.getSourceId(), "NA");
    }
  }

  private boolean isKilling(ExecutorId executorId) {
    if (context.getSchedulerExecutorManager() == null) {
      return false;
    }
    return (context.getSchedulerExecutorManager().getRunningExecutorMap()
        .get(executorId).getExecutorState() == SchedulerExecutorState.KILLING);
  }

  // TODO: Implement with ExecutorID instead
  private boolean isZombie(ExecutorId executorId) {
    Date launchTime = Optional.ofNullable(context)
        .map(AMContext::getSchedulerExecutorManager)
        .map(manager -> manager.getSchedulerExecutor(executorId))
        .map(SchedulerExecutor::getLaunchTime)
        .orElse(null);

    if (launchTime == null) {
      LOG.warn("Missing launch time for Executor[{}]", executorId);
      return false;
    }

    if (executorLatestLaunchTimes.containsKey(executorId) &&
        executorLatestLaunchTimes.get(executorId).after(launchTime)
    ) {
      LOG.warn("Caught zombie Executor[{}]", executorId);
      return true;
    }

    executorLatestLaunchTimes.put(executorId, launchTime);
    return false;
  }

  private void updateTaskAssignedInfo(TaskStatus taskStatus, ExecutorId executorId) {
    if (context.getSchedulerExecutorManager() != null) {
      SchedulerExecutor schedulerExecutor = context
          .getSchedulerExecutorManager()
          .getRunningExecutorMap()
          .get(executorId);
      taskStatus.setWorkerName(schedulerExecutor.getExecutorId().toUniqString());
      taskStatus.setAssignedNode(context
          .getMonitorInfoProvider()
          .getExecutorNodeName(schedulerExecutor));
      taskStatus.setAssignedNodeUrl(context
          .getMonitorInfoProvider()
          .getExecutorLogUrl(schedulerExecutor));
    }
  }

  /**
   * heartbeat is the main routine in driver that synchronizes states with executor in which driver
   * updates the states of previously assigned tasks and generates TaskCommands to executor to
   * achieve the new desired state.
   *
   * @param executorId                 the ID of the targeting executor.
   * @param taskStatusListFromExecutor task statuses reported by executor.
   * @param removeTask                 whether to move all tasks from the specified executor to
   *                                   pending tasks.
   * @param needMoreTask               whether to assign new tasks to the specified executor.
   * @return TaskCommands
   */
  public List<TaskCommand> heartbeat(
      ExecutorId executorId,
      List<TaskStatus> taskStatusListFromExecutor,
      boolean removeTask,
      boolean needMoreTask
  ) {
    LOG.info("Receiving heartbeat from {}", executorId);
    Map<Long, TaskStatus> taskStatusMapFromExecutor = taskStatusListFromExecutor.stream()
        .collect(Collectors.toMap(
            TaskStatus::getTaskId,
            taskStatus -> taskStatus
        ));

    // Handle zombie executor, which should have been replaces by a new executor, hence simply all
    // tasks reported by zombie executors and leave task state machines intact.
    if (isZombie(executorId)) {
      return computeTaskCommands(
          executorId,
          new HashMap<>(), // RunningTaskMapInDriver
          taskStatusMapFromExecutor
      );
    }

    // Update blocklist
    taskStatusListFromExecutor.stream()
        .filter(taskStatus -> taskStatus.getTaskState() == TaskState.FAILED)
        .forEach(taskStatus -> context
            .getSchedulerExecutorManager()
            .addTaskFailureBlacklist(taskStatus.getTaskId(), executorId)
        );

    // Handle suspended FileTaskManager
    if (isSuspended) {
      // Update TaskStore
      LOG.info("TaskManager is suspended, move tasks on executor[{}] to pending tasks", executorId);
      Map<Long, TaskWrapper> runningTasksInDriver = taskStore.updateAndRemoveTasks(
          executorId,
          taskStatusMapFromExecutor,
          maxTaskAttempts,
          -1 // taskAttemptModifier
      );
      return computeTaskCommands(executorId, runningTasksInDriver, taskStatusMapFromExecutor);
    }

    // Remove all tasks from executor if needed
    for (Entry<String, Boolean> entry : ImmutableMap.of(
        String.format("FileTaskManager[%s] has stopped", name), isStopped,
        "executor is requesting to remove all tasks", removeTask,
        "executor in killing state", isKilling(executorId),
        "executor is blacklisted", context.getSchedulerExecutorManager().isBlacklisted(executorId)
    ).entrySet()) {
      String reason = entry.getKey();
      boolean predicate = entry.getValue();
      if (predicate) {
        LOG.info("Remove tasks from Executor[{}] to pending tasks, reason: {}", executorId, reason);
        Map<Long, TaskWrapper> runningTasksInDriver = taskStore.updateAndRemoveTasks(
            executorId,
            taskStatusMapFromExecutor,
            maxTaskAttempts,
            0 // taskAttemptModifier
        );
        return computeTaskCommands(executorId, runningTasksInDriver, taskStatusMapFromExecutor);
      }
    }

    // Try assigning a new task
    Map<Long, TaskWrapper> runningTasksInDriver = taskStore
        .updateAndAssignTasks(
            executorId,
            taskStatusMapFromExecutor,
            maxTaskAttempts,
            maxTaskNumPerWorker,
            needMoreTask
        );

    runningTasksInDriver.values().forEach(taskWrapper -> {
      int sourceId = taskWrapper.getTask().getSourceId();
      String batchKey = Optional
          .of(taskWrapper)
          .map(TaskWrapper::getTask)
          .map(Task::getFileTask)
          .map(FileTask::getBatchKey)
          .orElse(DEFAULT_BATCH_KEY);
      dataSourceBatchKeyMap.merge(
          sourceId,
          batchKey,
          (a, b) -> a.compareTo(b) > 0 ? a : b // bigger key wins
      );
    });

    // Graceful shutdown executor if needed
    if (runningTasksInDriver.isEmpty() &&
        context.getApplicationMeta().getPrimusConf().getInputManager().getGracefulShutdown() &&
        taskBuilder.isFinished() &&
        taskStore.hasBeenExhausted()
    ) {
      LOG.info("No pending and running tasks, graceful shutdown executor({})", executorId);
      context.emitExecutorKillEvent(executorId);
      if (isSuccess()) {
        startTimeoutCheck();
      }
    }

    // Assemble TaskCommands
    return computeTaskCommands(executorId, runningTasksInDriver, taskStatusMapFromExecutor);
  }

  private List<TaskCommand> computeTaskCommands(
      ExecutorId executorId,
      Map<Long, TaskWrapper> runningTasksInDriver,
      Map<Long, TaskStatus> taskStatusesFromExecutor
  ) {
    List<TaskCommand> taskAssigmentCommands = runningTasksInDriver.entrySet().stream()
        .flatMap(entry -> taskStatusesFromExecutor.containsKey(entry.getKey())
            ? Stream.empty()
            : Stream.of(newTaskAssignmentCommand(executorId, entry.getValue()))
        )
        .collect(Collectors.toList());

    List<TaskCommand> taskRemovalCommands = taskStatusesFromExecutor.entrySet().stream()
        .flatMap(entry -> runningTasksInDriver.containsKey(entry.getKey())
            ? Stream.empty()
            : Stream.of(newTaskRemovalCommand(entry.getKey()))
        )
        .collect(Collectors.toList());

    return new ArrayList<TaskCommand>() {{
      addAll(taskAssigmentCommands);
      addAll(taskRemovalCommands);
    }};
  }

  private TaskCommand newTaskAssignmentCommand(ExecutorId executorId, TaskWrapper taskWrapper) {
    updateTaskAssignedInfo(taskWrapper.getTaskStatus(), executorId);
    TaskCommand taskCommand = new TaskCommandPBImpl();
    taskCommand.setTaskCommandType(TaskCommandType.ASSIGN);
    taskCommand.setTask(taskWrapper.getTask());
    return taskCommand;
  }

  private TaskCommand newTaskRemovalCommand(long taskId) {
    Task task = new TaskPBImpl();
    task.setGroup(name);
    task.setTaskId(taskId);
    TaskCommand taskCommand = new TaskCommandPBImpl();
    taskCommand.setTaskCommandType(TaskCommandType.REMOVE);
    taskCommand.setTask(task);
    return taskCommand;
  }

  @Override
  public void unregister(ExecutorId executorId) {
    if (isZombie(executorId)) {
      LOG.warn("Zombie executor" + executorId + "] is unregistering");
      return;
    }

    LOG.info("Executor [" + executorId + "] unregistering...");
    executorLatestLaunchTimes.remove(executorId);
    taskStore.updateAndRemoveTasks(
        executorId,
        new HashMap<>(),
        maxTaskAttempts,
        0 // taskAttemptModifier
    );
  }

  @Override
  public void suspend(int snapshotId) {
    LOG.info("FileTaskManager suspend");
    this.isSuspended = true;
    this.snapshotId = snapshotId;
  }

  @Override
  public SuspendStatusEnum getSuspendStatus() {
    return !isSuspended
        ? SuspendStatusEnum.NOT_STARTED
        : taskStore.hasRunningTasks()
            ? SuspendStatusEnum.RUNNING
            : SuspendStatusEnum.FINISHED_SUCCESS;
  }

  @Override
  public void resume() {
    LOG.info("FileTaskManager resume");
    this.isSuspended = false;
  }

  @Override
  public boolean isSuspend() {
    return isSuspended;
  }

  @Override
  public int getSnapshotId() {
    return snapshotId;
  }

  @Override
  public boolean takeSnapshot(String directory) {
    return taskStore.takeSnapshot(directory);
  }

  @Override
  public List<TaskWrapper> getTasksForHistory() {
    return new LinkedList<TaskWrapper>() {{
      addAll(taskStore.getSuccessTasks(0));
      addAll(taskStore.getFailureTasks(HISTORY_FAILURE_TASKS_LIMIT_COUNT));
      addAll(taskStore.getRunningTasks().values());
      addAll(taskStore.getPendingTasks(10000));
    }};
  }

  @Override
  public TaskManagerState getState() {
    if (isSuccess()) {
      return TaskManagerState.SUCCEEDED;
    } else if (isFailure()) {
      return TaskManagerState.FAILED;
    }
    return TaskManagerState.RUNNING;
  }

  @Override
  public float getProgress() {
    TaskStatistics statistics = taskStore.getTaskStatistics();
    long totalTaskNum = statistics.getTotalTaskNum();
    long finishTaskNum = statistics.getSuccessTaskNum() + statistics.getFailureTaskNum();

    if (totalTaskNum <= 0) {
      LOG.warn("Can't get progress because tasks number is under calculating");
      return -1;
    }

    // Apply a modifier of 20% if task builder hasn't finished.
    float modifier = taskBuilder.isFinished() ? 1.0f : 0.2f;
    return modifier * finishTaskNum / totalTaskNum;
  }

  /**
   * Returns the current batch key of each DataSource.
   */
  @Override
  public Map<Integer, String> getDataSourceReports() {
    // Assemble output
    return dataSourceBatchKeyMap.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> "Processing BatchKey: " + e.getValue()
        ));
  }

  public Collection<TaskWrapper> getPendingTasks(int size) {
    return taskStore.getPendingTasks(size);
  }

  @Override
  public boolean isFailure() {
    if (!taskBuilder.isFinished()) {
      return false;
    }

    TaskStatistics statistics = taskStore.getTaskStatistics();
    PrimusMetrics.emitStoreWithAppIdTag("total_task_num", statistics.getTotalTaskNum());
    PrimusMetrics.emitStoreWithAppIdTag("pending_task_num", statistics.getPendingTaskNum());
    PrimusMetrics.emitStoreWithAppIdTag("success_task_num", statistics.getSuccessTaskNum());
    PrimusMetrics.emitStoreWithAppIdTag("failed_task_num", statistics.getFailureTaskNum());

    // we hope that even if a few worker is stuck (neither success nor fail), the app can still finish.
    // we also hope that the failed task is not to much when the app tells me it's success.
    if (statistics.getTotalTaskNum() > 0 &&
        taskFailedPercent > 0 &&
        taskFailedPercent <= statistics.getFailureTaskNum() * 100.0 / statistics.getTotalTaskNum()
    ) {
      LOG.info("Reach to failed task percent " + taskFailedPercent + "%"
          + ", total task number: " + statistics.getTotalTaskNum()
          + ", success task number: " + statistics.getSuccessTaskNum()
          + ", failed task number: " + statistics.getFailureTaskNum()
      );
      return true;
    }

    return false;
  }

  @Override
  public boolean isSuccess() {
    if (!taskBuilder.isFinished()) {
      return false;
    }

    if (isSucceed) {
      return true;
    }

    TaskStatistics statistics = taskStore.getTaskStatistics();
    PrimusMetrics.emitStoreWithAppIdTag("total_task_num", statistics.getTotalTaskNum());
    PrimusMetrics.emitStoreWithAppIdTag("pending_task_num", statistics.getPendingTaskNum());
    PrimusMetrics.emitStoreWithAppIdTag("success_task_num", statistics.getSuccessTaskNum());
    PrimusMetrics.emitStoreWithAppIdTag("failed_task_num", statistics.getFailureTaskNum());

    // check success task percent
    // TODO: Rename this feature to complete percent
    double completeTaskNum = statistics.getSuccessTaskNum() + statistics.getFailureTaskNum();
    if (statistics.getTotalTaskNum() > 0 &&
        taskSuccessPercent > 0 &&
        taskSuccessPercent <= completeTaskNum * 100.0 / statistics.getTotalTaskNum()
    ) {
      LOG.info("Reach to success task percent " + taskSuccessPercent + "%"
          + ", total task number: " + statistics.getTotalTaskNum()
          + ", success task number: " + statistics.getSuccessTaskNum()
          + ", failed task number: " + statistics.getFailureTaskNum()
      );
      return true;
    }

    // check if all tasks are finished
    if (!taskStore.hasBeenExhausted()) {
      return false;
    }

    LOG.info(
        "Finish all tasks, total: {}, success: {}, failed: {}, lost: {}",
        statistics.getTotalTaskNum(),
        statistics.getSuccessTaskNum(),
        statistics.getFailureTaskNum(),
        statistics.getTotalTaskNum() - completeTaskNum
    );
    return true;
  }

  private void startTimeoutCheck() {
    if (!isTimeoutChecking) {
      Thread thread = new Thread(
          () -> {
            long startTime = System.currentTimeMillis();
            long timeoutMillis = IntegerUtils.ensurePositiveOrDefault(
                context.getApplicationMeta().getPrimusConf().getInputManager()
                    .getGracefulShutdownTimeWaitSec(),
                DEFAULT_GRACEFUL_SHUTDOWN_TIME_WAIT_SEC
            );

            while ((System.currentTimeMillis() - startTime) < timeoutMillis * 1000) {
              try {
                Thread.sleep(DEFAULT_GRACEFUL_SHUTDOWN_TIME_CHECK_SEC);
              } catch (InterruptedException e) {
                // ignore
              }
            }
            failApp("There is no pending task and task manager is timeout",
                ApplicationExitCode.TIMEOUT_WITH_NO_PENDING_TASK.getValue());
          }
      );
      thread.setDaemon(true);
      thread.start();
      isTimeoutChecking = true;
    }
  }

  private void failApp(String diag, int exitCode) {
    LOG.error(diag);
    context.emitFailApplicationEvent(diag, exitCode);
  }

  @Override
  public void stop() {
    isStopped = true;
    LOG.info("stop...");
    while (taskStore.hasRunningTasks()) {
      LOG.info("have some tasks running so waiting");
      Sleeper.sleepWithoutInterruptedException(Duration.ofSeconds(5));
    }
    LOG.info("no running task");
    taskBuilder.stop();
    LOG.info("task builder stopped");
    taskStore.stop();
    LOG.info("task store stopped");
  }

  @Override
  public void succeed() {
    LOG.info("FileTaskManager succeed");
    this.isSucceed = true;
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
