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

import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_STATE_FAILED_ATTEMPT_EVENT;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_STATE_FAILED_EVENT;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_STATE_SUCCESS_EVENT;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMasterEvent;
import com.bytedance.primus.am.ApplicationMasterEventType;
import com.bytedance.primus.am.controller.SuspendStatusEnum;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.TaskManagerState;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.exception.PrimusAMException;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEventType;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorState;
import com.bytedance.primus.api.records.ExecutorId;
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
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.proto.PrimusInput.InputManager;
import com.bytedance.primus.utils.PrimusConstants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTaskManager implements TaskManager {

  private static final int DEFAULT_TIMEOUT_MIN_AFTER_FINISH = 30;
  public static final int HISTORY_FAILURE_TASKS_LIMIT_COUNT = 10000;

  private Logger LOG;
  private AMContext context;
  private String name;
  private DataStreamSpec dataStreamSpec;
  private Map<ExecutorId, Date> executorLatestLaunchTimes;

  private Map<Long, TaskStatus> preservedTaskStatuses;
  private TaskStore taskStore;
  private FileTaskBuilder taskBuilder;
  private final ReentrantReadWriteLock readWriteLock;

  private int maxTaskNumPerWorker;
  private int maxTaskAttempts;
  private float taskSuccessPercent;
  private float taskFailedPercent;

  private volatile boolean isSuspended = false;
  private volatile int snapshotId = 0;
  private Random random = new Random();
  private volatile boolean isTimeoutChecking = false;

  private Map<String, TaskWrapper> newlyAssignedTasks;
  private Map<String, Integer> dataConsumptionTimeMap;

  private long version;
  private volatile boolean isStopped = false;
  private volatile boolean isSucceed = false;

  public FileTaskManager(AMContext context, String name, DataStreamSpec dataStreamSpec,
      String savepointDir, long version)
      throws IOException, PrimusAMException {
    this.version = version;
    this.LOG = LoggerFactory.getLogger(FileTaskManager.class.getName() + "[" + name + "]");
    this.context = context;
    this.name = name;
    this.dataStreamSpec = dataStreamSpec;
    this.readWriteLock = new ReentrantReadWriteLock();
    this.executorLatestLaunchTimes = new ConcurrentHashMap<>();
    this.preservedTaskStatuses = null;
    this.taskStore = new FileTaskStore(context, name, savepointDir, readWriteLock);
    this.taskBuilder = new FileTaskBuilder(context, name, dataStreamSpec, taskStore);

    InputManager inputManager = context.getPrimusConf().getInputManager();
    maxTaskNumPerWorker = inputManager.getMaxTaskNumPerWorker();
    if (maxTaskNumPerWorker <= 0) {
      maxTaskNumPerWorker = PrimusConstants.DEFAULT_MAX_TASK_NUM_PER_WORKER;
    }
    maxTaskAttempts = inputManager.getMaxTaskAttempts();
    if (maxTaskAttempts <= 0) {
      maxTaskAttempts = PrimusConstants.DEFAULT_MAX_TASK_ATTEMPTS;
    }
    taskSuccessPercent = inputManager.getStopPolicy().getTaskSuccessPercent();
    taskFailedPercent = inputManager.getStopPolicy().getTaskFailedPercent();
    if (taskFailedPercent <= 0) {
      taskFailedPercent = PrimusConstants.DEFAULT_FAILED_PERCENT;
    }

    newlyAssignedTasks = new ConcurrentHashMap<>();
    dataConsumptionTimeMap = new ConcurrentHashMap<>();
    for (DataSourceSpec dataSourceSpec : dataStreamSpec.getDataSourceSpecs()) {
      dataConsumptionTimeMap.put(dataSourceSpec.getSourceId(), 0);
    }
  }

  private Date getSchedulerExecutorLaunchTime(ExecutorId executorId) {
    if (context.getSchedulerExecutorManager() != null) {
      return context.getSchedulerExecutorManager().getSchedulerExecutor(executorId).getLaunchTime();
    }
    return null;
  }

  private boolean isBlacklisted(AMContext context, ExecutorId executorId) {
    if (context.getExecutorNodeMap() != null && context.getBlacklistTrackerOpt() != null) {
      String node = context.getExecutorNodeMap().get(executorId.toUniqString());
      return context.getBlacklistTrackerOpt()
          .map(b -> b.isContainerBlacklisted(executorId.toUniqString()) ||
              b.isNodeBlacklisted(node)
          ).orElse(false);
    }
    return false;
  }

  private boolean isKilling(AMContext context, ExecutorId executorId) {
    if (context.getSchedulerExecutorManager() == null) {
      return false;
    }
    return (context.getSchedulerExecutorManager().getRunningExecutorMap()
        .get(executorId).getExecutorState() == SchedulerExecutorState.KILLING);
  }

  private boolean isEvaluationRole(AMContext context, ExecutorId executorId) {
    if (context.getRoleInfoManager() == null) {
      return false;
    }
    return context.getRoleInfoManager().getRoleNameRoleInfoMap().get(executorId.getRoleName())
        .getRoleSpec().getExecutorSpecTemplate().getIsEvaluation();
  }

  private boolean isZombie(ExecutorId executorId) {
    boolean isZombieExecutor = false;
    Date launchTime = getSchedulerExecutorLaunchTime(executorId);
    if (launchTime != null) {
      if (executorLatestLaunchTimes.containsKey(executorId)) {
        if (launchTime.before(executorLatestLaunchTimes.get(executorId))) {
          isZombieExecutor = true;
        } else {
          executorLatestLaunchTimes.put(executorId, launchTime);
        }
      } else {
        executorLatestLaunchTimes.put(executorId, launchTime);
      }
    }
    return isZombieExecutor;
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

  private void addTaskFailureBlacklist(AMContext context, long taskId, ExecutorId executorId) {
    if (context.getExecutorNodeMap() != null && context.getBlacklistTrackerOpt() != null) {
      String node = context.getExecutorNodeMap().get(executorId.toUniqString());
      context.getBlacklistTrackerOpt().ifPresent(b -> b.addTaskFailure(
          Long.toString(taskId),
          executorId.toUniqString(),
          node,
          System.currentTimeMillis()));
    }
  }

  private void preHeartbeat(ExecutorId executorId) {
    if (taskStore.getRunningTasks(executorId) == null) {
      readWriteLock.readLock().lock();
      if (taskStore.getRunningTasks(executorId) == null) {
        taskStore.addExecutorRunningTasks(executorId, new ConcurrentHashMap<>());
      }
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public List<TaskCommand> heartbeat(
      ExecutorId executorId,
      List<TaskStatus> taskStatuses,
      boolean removeTask,
      boolean needMoreTask
  ) {
    LOG.debug("Receiving heartbeat from {}", executorId);

    // Preprocess
    TimerMetric latency = PrimusMetrics.getTimerContextWithOptionalPrefix(
        "am.taskmanager.heartbeat.latency");

    preHeartbeat(executorId);
    List<TaskCommand> taskCommands = new ArrayList<>();

    if (isStopped) {
      for (TaskStatus taskStatus : taskStatuses) {
        LOG.info("TaskManager stop, send remove task[" + taskStatus.getTaskId()
            + "] request to executor["
            + executorId + "]");
        addRemoveTaskCommand(taskStatus.getTaskId(), taskCommands);
      }
      Map<Long, TaskWrapper> taskMapInAm = taskStore.getRunningTasks(executorId);
      synchronized (taskMapInAm) {
        if (taskMapInAm != null) {
          for (Map.Entry<Long, TaskWrapper> entry : taskMapInAm.entrySet()) {
            LOG.info(
                "TaskManager stop, remove task[" + entry.getKey() + "] on executor[" + executorId
                    + "] and add to pending tasks");
            taskStore.addPendingTasks(entry.getValue());
            taskMapInAm.remove(entry.getKey());
          }
        }
      }
      return taskCommands;
    }

    boolean isBlacklisted = isBlacklisted(context, executorId);
    boolean isExecutorKilling = isKilling(context, executorId);
    if (isBlacklisted) {
      LOG.info("executorId[" + executorId + "] is in blacklist. Cannot assign new task");
    } else if (isExecutorKilling) {
      LOG.info("executorId[" + executorId + "] is in killing. Cannot assign new task");
    }
    isExecutorKilling = isBlacklisted || isExecutorKilling;
    boolean isEvaluation = isEvaluationRole(context, executorId);
    boolean isZombieExecutor = isZombie(executorId);

    Map<Long, TaskWrapper> taskMapInAm = taskStore.getRunningTasks(executorId);
    synchronized (taskMapInAm) {
      if (taskMapInAm == null || isZombieExecutor) {
        LOG.warn("unregister executor[" + executorId + ", remove task on it");
        for (TaskStatus taskStatus : taskStatuses) {
          LOG.info(
              "send remove task[" + taskStatus.getTaskId() + "] request to executor[" + executorId
                  + "]");
          addRemoveTaskCommand(taskStatus.getTaskId(), taskCommands);
        }
      } else {
        Map<Long, TaskStatus> taskStatusMapInExecutor =
            taskStatuses.stream()
                .collect(Collectors.toMap(TaskStatus::getTaskId, taskStatus -> taskStatus));

        // prevent tasks preservation since we are going to modify states of some tasks
        readWriteLock.readLock().lock();

        // assign task from pending to executor
        Set<Long> newlyPollTasks = new HashSet<>();
        // if suspended do not add new task to executor
        if (!isExecutorKilling && !isSuspended && needMoreTask
            && taskMapInAm.size() < maxTaskNumPerWorker) {
          TaskWrapper taskWrapper = pollPendingTask(executorId);
          if (taskWrapper != null) {
            taskMapInAm.put(taskWrapper.getTask().getTaskId(), taskWrapper);
            newlyPollTasks.add(taskWrapper.getTask().getTaskId());
            newlyAssignedTasks.put(taskWrapper.getTask().getSourceId(), taskWrapper);
          }
        }

        for (Map.Entry<Long, TaskWrapper> entry : taskMapInAm.entrySet()) {
          TaskWrapper taskWrapper = entry.getValue();
          TaskStatus taskStatus = taskWrapper.getTaskStatus();

          // assign task not existed in executor
          if (!isSuspended && !isExecutorKilling && !taskStatusMapInExecutor.containsKey(
              entry.getKey())) {
            if (needMoreTask) {
              LOG.info("send assign task[" + entry.getKey() + "] request to executor[" + executorId
                  + "]");
              if (newlyPollTasks.contains(entry.getKey()) && !isEvaluation) {
                int numAttempt = taskWrapper.getTask().getNumAttempt();
                taskWrapper.getTask().setNumAttempt(numAttempt + 1);
                taskWrapper.getTaskStatus().setNumAttempt(numAttempt + 1);
              }
              updateTaskAssignedInfo(taskStatus, executorId);
              TaskCommand taskCommand = new TaskCommandPBImpl();
              taskCommand.setTaskCommandType(TaskCommandType.ASSIGN);
              taskCommand.setTask(taskWrapper.getTask());
              taskCommands.add(taskCommand);
            } else {
              LOG.info("Executor do not need more task. remove task[" + entry.getKey() +
                  "] on executor[" + executorId + "] and add to pending tasks");
              taskStore.addPendingTasks(taskWrapper);
              taskMapInAm.remove(entry.getKey());
            }
          }

          // recycle failed task in executor
          if (taskStatus.getTaskState() == TaskState.FAILED) {
            context.getTimelineLogger()
                .logEvent(PRIMUS_TASK_STATE_FAILED_ATTEMPT_EVENT.name(),
                    taskWrapper.getTask().toString());
            addTaskFailureBlacklist(context, taskStatus.getTaskId(), executorId);
            if (taskWrapper.getTask().getNumAttempt() >= maxTaskAttempts) {
              LOG.info("Task failure times reaches to maxTaskAttempts " + maxTaskAttempts
                  + ", remove task[" + entry.getKey() + "] on executor[" + executorId + "]");
              failTask(taskWrapper);
            } else {
              LOG.info("Task fail, remove task[" + entry.getKey() + "] on executor[" + executorId
                  + "] and add to pending tasks");
              taskStatus.setTaskState(TaskState.RUNNING);
              taskStore.addPendingTasks(taskWrapper);
            }
            taskMapInAm.remove(entry.getKey());
          }

          // remove succeeded task in executor
          if (taskStatus.getTaskState() == TaskState.SUCCEEDED) {
            context.getTimelineLogger()
                .logEvent(PRIMUS_TASK_STATE_SUCCESS_EVENT.name(), taskWrapper.getTask().toString());
            LOG.info("Task succeed, remove task[" + entry.getKey() + "] on executor[" + executorId
                + "] and add to finished tasks");
            taskStatus.setFinishTime(new Date().getTime());
            taskStore.addSuccessTask(taskWrapper);
            taskMapInAm.remove(entry.getKey());
          }

          if (isSuspended && taskMapInAm.containsKey(entry.getKey())) {
            LOG.info("Executor isSuspended: " + isSuspended
                + ", remove task[" + entry.getKey()
                + "] on executor[" + executorId + "] and add to pending tasks");
            int numAttempt = taskWrapper.getTask().getNumAttempt();
            taskWrapper.getTask().setNumAttempt(numAttempt - 1);
            taskWrapper.getTaskStatus().setNumAttempt(numAttempt - 1);
            taskStore.addPendingTasks(taskWrapper);
            taskMapInAm.remove(entry.getKey());
          }
        }

        for (Map.Entry<Long, TaskStatus> entry : taskStatusMapInExecutor.entrySet()) {
          if (taskMapInAm.containsKey(entry.getKey())) {
            // update status and prevent to overwrite assigned info
            TaskWrapper taskWrapper = taskMapInAm.get(entry.getKey());
            TaskStatus oldTaskStatus = taskWrapper.getTaskStatus();
            taskWrapper.setTaskStatus(entry.getValue());
            taskWrapper.getTaskStatus().setWorkerName(executorId.toUniqString());
            taskWrapper.getTaskStatus().setAssignedNode(oldTaskStatus.getAssignedNode());
            taskWrapper.getTaskStatus().setAssignedNodeUrl(oldTaskStatus.getAssignedNodeUrl());
            taskWrapper.getTask().setCheckpoint(entry.getValue().getCheckpoint());
            taskWrapper.getTask().setNumAttempt(entry.getValue().getNumAttempt());

            if ((isEvaluation && !needMoreTask
                && taskWrapper.getTaskStatus().getTaskState() == TaskState.RUNNING)
                || removeTask) {
              // recycle tasks
              LOG.info("Recycle task[" + entry.getKey() + "] executor[" + executorId
                  + "], isEvaluation: " + isEvaluation + ", removeTask: " + removeTask);
              taskStore.addPendingTasks(taskWrapper);
              taskMapInAm.remove(entry.getKey());
              addRemoveTaskCommand(entry.getKey(), taskCommands);
            }
          } else {
            // remove task not existed in am task manager
            LOG.info(
                "send remove task[" + entry.getKey() + "] request to executor[" + executorId + "]");
            addRemoveTaskCommand(entry.getKey(), taskCommands);
          }
        }

        readWriteLock.readLock().unlock();
      }
    }
    latency.stop();
    return taskCommands;
  }

  @Override
  public void unregister(ExecutorId executorId) {
    if (isZombie(executorId)) {
      LOG.info("Zombie executor" + executorId + "] is unregistering");
      return;
    }
    LOG.info("Executor [" + executorId + "] unregistering...");
    executorLatestLaunchTimes.remove(executorId);
    readWriteLock.readLock().lock();
    Map<Long, TaskWrapper> tasks = taskStore.removeExecutorRunningTasks(executorId);
    if (tasks != null && !tasks.isEmpty()) {
      // recycle the tasks in unregistered executor
      for (TaskWrapper taskWrapper : tasks.values()) {
        switch (taskWrapper.getTaskStatus().getTaskState()) {
          case RUNNING:
            if (taskWrapper.getTask().getNumAttempt() >= maxTaskAttempts) {
              LOG.info("[Unregister]Task attempt times reaches to maxTaskAttempts "
                  + maxTaskAttempts + ", remove task[" + taskWrapper.getTask().getTaskId()
                  + "] on executor[" + executorId + "]");
              failTask(taskWrapper);
            } else {
              taskStore.addPendingTasks(taskWrapper);
            }
            break;
          case SUCCEEDED:
            context.getTimelineLogger()
                .logEvent(PRIMUS_TASK_STATE_SUCCESS_EVENT.name(), taskWrapper.getTask().toString());
            LOG.info("[Unregister]Task succeed, remove task[" + taskWrapper.getTask().getTaskId()
                + "] on executor[" + executorId + "] and add to finished tasks");
            taskWrapper.getTaskStatus().setFinishTime(new Date().getTime());
            taskStore.addSuccessTask(taskWrapper);
            break;
          case FAILED:
            context.getTimelineLogger()
                .logEvent(PRIMUS_TASK_STATE_FAILED_ATTEMPT_EVENT.name(),
                    taskWrapper.getTask().toString());
            if (taskWrapper.getTask().getNumAttempt() >= maxTaskAttempts) {
              LOG.info("[Unregister]Task failure times reaches to maxTaskAttempts "
                  + maxTaskAttempts + ", remove task[" + taskWrapper.getTask().getTaskId()
                  + "] on executor[" + executorId + "]");
              failTask(taskWrapper);
            } else {
              LOG.info("[Unregister]Task fail, remove task[" + taskWrapper.getTask().getTaskId()
                  + "] on executor[" + executorId + "] and add to pending tasks");
              taskWrapper.getTaskStatus().setTaskState(TaskState.RUNNING);
              taskStore.addPendingTasks(taskWrapper);
            }
            break;
        }
      }
    }
    readWriteLock.readLock().unlock();
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

  private void failTask(TaskWrapper taskWrapper) {
    context.getTimelineLogger()
        .logEvent(PRIMUS_TASK_STATE_FAILED_EVENT.name(), taskWrapper.getTask().toString());
    taskWrapper.getTaskStatus().setTaskState(TaskState.FAILED);
    taskWrapper.getTaskStatus().setFinishTime(new Date().getTime());
    taskStore.addFailureTask(taskWrapper);
  }

  @Override
  public void suspend(int snapshotId) {
    LOG.info("FileTaskManager suspend");
    this.isSuspended = true;
    this.snapshotId = snapshotId;
  }

  @Override
  public SuspendStatusEnum getSuspendStatus() {
    if (!this.isSuspended) {
      return SuspendStatusEnum.NOT_STARTED;
    }
    if (taskStore.isNoRunningTask()) {
      return SuspendStatusEnum.FINISHED_SUCCESS;
    } else {
      return SuspendStatusEnum.RUNNING;
    }
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
  public boolean makeSavepoint(String savepointDir) {
    return taskStore.makeSavepoint(savepointDir);
  }

  @Override
  @Deprecated()
  public List<TaskWrapper> getTasksForTaskPreserverSnapshot() {
    List<TaskWrapper> tasks = new LinkedList();
    tasks.addAll(taskStore.getSuccessTasks());
    tasks.addAll(taskStore.getFailureTasks());
    tasks.addAll(getRunningAndPendingTaskWrappers());
    return tasks;
  }

  @Override
  public List<TaskWrapper> getTasksForHistory() {
    List<TaskWrapper> tasks = new LinkedList();
    tasks.addAll(taskStore.getSuccessTasks());
    tasks.addAll(taskStore.getFailureTasksWithLimit(HISTORY_FAILURE_TASKS_LIMIT_COUNT));
    tasks.addAll(getRunningAndPendingTaskWrappers());
    return tasks;
  }

  private List<TaskWrapper> getRunningAndPendingTaskWrappers() {
    List<TaskWrapper> tasks = new LinkedList();
    for (Map<Long, TaskWrapper> taskMap : taskStore.getExecutorRunningTaskMap().values()) {
      tasks.addAll(taskMap.values());
    }
    tasks.addAll(taskStore.getPendingTasks(10000));
    return tasks;
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
    float progress;
    int finishTaskNum = getFinishTaskNum();
    int totalTaskNum = getTotalTaskNum();
    int currentTaskNum = getCurrentTaskNum();
    if (totalTaskNum > 0) {
      progress = finishTaskNum * 1.0f / totalTaskNum;
    } else if (currentTaskNum > 0) {
      // a fake progress, no more than 20%
      progress = finishTaskNum * 1.0f / currentTaskNum * 0.2f;
    } else {
      LOG.warn("Can't get progress because tasks number is under calculating");
      progress = -1;
    }
    return progress;
  }

  /**
   * Return a newly data consumption time of each data source.
   *
   * @return a Map contains newly data consumption time of each data source where key is data source
   * id and value is the data consumption time with format YYYYMMDDHH.
   */
  @Override
  public Map<String, Integer> getDataConsumptionTimeMap() { // TODO: Maybe deprecate this map
    for (Map.Entry<String, TaskWrapper> entry : newlyAssignedTasks.entrySet()) {
      String sourceId = entry.getKey();
      Task task = entry.getValue().getTask();
      int dayHour = 0;
      try {
        // key is YYYYMMDD(day granularity) or YYYYMMDDHH(hour granularity)
        // or YYYYMMDD#*(generated by com.bytedance.primus.am.datastream.file.operator.op.MapDelay)
        // or YYYYMMDDHH#*(generated by com.bytedance.primus.am.datastream.file.operator.op.MapDelay)
        String key;
        if (task.getSplitTask() != null) {
          key = task.getSplitTask().getKey().split("#")[0];
        } else {
          key = "";
        }
        if (key.length() == 8) {  // YYYYMMDD
          dayHour = Integer.valueOf(key) * 100;
        } else if (key.length() == 10) {  // YYYYMMDDHH
          dayHour = Integer.valueOf(key);
        } else {
          return dataConsumptionTimeMap;
        }
      } catch (Exception e) {
        LOG.error("Failed to get integer from string", e);
      }
      int dataConsumptionTime = dataConsumptionTimeMap.get(sourceId);
      dataConsumptionTime = Math.max(dataConsumptionTime, dayHour);
      dataConsumptionTimeMap.put(sourceId, dataConsumptionTime);
    }
    return dataConsumptionTimeMap;
  }

  public List<TaskWrapper> getPendingTasks(int size) {
    return taskStore.getPendingTasks(size);
  }

  public int getTotalTaskNum() {
    if (taskBuilder.isFinished()) {
      return taskStore.getTotalTaskNum();
    }
    return 0;
  }

  public int getCurrentTaskNum() {
    return taskStore.getTotalTaskNum();
  }

  public int getFinishTaskNum() {
    return taskStore.getSuccessTaskNum() + taskStore.getFailureTaskNum();
  }

  public boolean isSnapshotAvailable(int snapshotId) {
    return ((FileTaskStore) taskStore).isSnapshotAvailable(snapshotId);
  }

  public String getSnapshotDir(int snapshotId) {
    return ((FileTaskStore) taskStore).getSnapshotDir(snapshotId);
  }

  private TaskWrapper pollPendingTask(ExecutorId executorId) {
    float sampleRate = context.getPrimusConf().getInputManager().getSampleRate();
    TaskWrapper taskWrapper = sampleRate <= 0
        ? taskStore.pollPendingTask()
        : pollPendingTaskWithSample(sampleRate);

    if (context.getPrimusConf().getInputManager().getGracefulShutdown()) {
      boolean noPendingTasks =
          taskBuilder.isFinished() && taskStore.getPendingTaskNum() <= 0 && taskWrapper == null;
      int runningTaskCount = taskStore.getRunningTasks(executorId).size();
      boolean noRunningTasks = runningTaskCount == 0;
      if (noPendingTasks && noRunningTasks) {
        LOG.info("No pending tasks and No running tasks, graceful shutdown executor " + executorId);
        gracefulShutdown(executorId, isSuccess());
      }
    }
    return taskWrapper;
  }

  private TaskWrapper pollPendingTaskWithSample(float sampleRate) {
    while (true) {
      TaskWrapper taskWrapper = taskStore.pollPendingTask();
      if (taskWrapper == null || taskWrapper.getTaskStatus().getNumAttempt() > 0) {
        return taskWrapper;
      }
      if (random.nextFloat() * 100 <= sampleRate) {
        return taskWrapper;
      }
      TaskStatus taskStatus = taskWrapper.getTaskStatus();
      taskStatus.setTaskState(TaskState.SUCCEEDED);
      taskStore.addSuccessTask(taskWrapper);
      LOG.info("Skip task, " +
          "taskId[" + taskWrapper.getTaskStatus().getTaskId() + "]." +
          "checkpoint[" + taskWrapper.getTaskStatus().getCheckpoint() + "]." +
          "progress[" + taskWrapper.getTaskStatus().getProgress() + "]." +
          "taskState[" + taskWrapper.getTaskStatus().getTaskState() + "]." +
          "numAttempt[" + taskWrapper.getTaskStatus().getNumAttempt() + "]." +
          "lastAssignTime[" + taskWrapper.getTaskStatus().getLastAssignTime() + "].");
    }
  }

  @Override
  public boolean isFailure() {
    if (!taskBuilder.isFinished()) {
      return false;
    }

    int totalTaskNum = taskStore.getTotalTaskNum();
    int failedTaskNum = taskStore.getFailureTaskNum();
    if (totalTaskNum > 0) {
      double currentTaskFailedPercent = failedTaskNum * 100.0 / totalTaskNum;
      // we hope that even if a few worker is stuck (neither success nor fail), the app can still finish.
      // we also hope that the failed task is not to much when the app tells me it's success.
      if (taskFailedPercent > 0 && currentTaskFailedPercent >= taskFailedPercent) {
        LOG.info("Reach to failed task percent " + taskFailedPercent
            + "%, failed task number " + failedTaskNum
            + ", current total task number " + totalTaskNum);
        return true;
      }
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

    int totalTaskNum = taskStore.getTotalTaskNum();
    int successTaskNum = taskStore.getSuccessTaskNum();
    int failedTaskNum = taskStore.getFailureTaskNum();
    int pendingTaskNum = taskStore.getPendingTaskNum();
    PrimusMetrics.emitStoreWithOptionalPrefix("total_task_num", totalTaskNum);
    PrimusMetrics.emitStoreWithOptionalPrefix("success_task_num", successTaskNum);
    PrimusMetrics.emitStoreWithOptionalPrefix("failed_task_num", failedTaskNum);
    PrimusMetrics.emitStoreWithOptionalPrefix("pending_task_num", pendingTaskNum);

    // check success task percent
    if (totalTaskNum > 0) {
      double currentTaskSuccessPercent = successTaskNum * 100.0 / totalTaskNum;
      double currentTaskFailedPercent = failedTaskNum * 100.0 / totalTaskNum;
      if (taskSuccessPercent > 0
          && currentTaskSuccessPercent + currentTaskFailedPercent >= taskSuccessPercent) {
        LOG.info("Reach to success task percent " + taskSuccessPercent
            + "%, success task number " + successTaskNum
            + ", fail task number " + failedTaskNum
            + ", total task number " + totalTaskNum);
        return true;
      }
    }

    // check if finish all tasks
    if (taskStore.getPendingTaskNum() <= 0) {
      boolean finish = true;
      for (ExecutorId executorId : executorLatestLaunchTimes.keySet()) {
        Map<Long, TaskWrapper> tasks = taskStore.getRunningTasks(executorId);
        if (tasks != null && !tasks.isEmpty()) {
          finish = false;
          break;
        }
      }
      // double check of pending task number
      if (taskStore.getPendingTaskNum() <= 0 && finish) {
        successTaskNum = taskStore.getSuccessTaskNum();
        failedTaskNum = taskStore.getFailureTaskNum();
        int lostTaskNum = totalTaskNum - successTaskNum - failedTaskNum;
        LOG.info("Finish all tasks, total tasks " + totalTaskNum
            + ", success tasks " + successTaskNum
            + ", failure tasks " + failedTaskNum
            + ", lost tasks " + lostTaskNum);
        return true;
      }
    }
    return false;
  }

  private void gracefulShutdown(ExecutorId executorId, boolean checkTimeout) {
    context.getDispatcher().getEventHandler().handle(
        new SchedulerExecutorManagerEvent(SchedulerExecutorManagerEventType.EXECUTOR_KILL,
            executorId));
    if (checkTimeout) {
      startTimeoutCheck();
    }
  }

  private void startTimeoutCheck() {
    if (!isTimeoutChecking) {
      Thread thread = new Thread(
          () -> {
            long startTime = System.currentTimeMillis();
            int timeoutMin = context.getPrimusConf().getInputManager()
                .getTimeoutMinAfterFinish();
            if (timeoutMin <= 0) {
              timeoutMin = DEFAULT_TIMEOUT_MIN_AFTER_FINISH;
            }
            int timeoutMs = timeoutMin * 60 * 1000;
            while ((System.currentTimeMillis() - startTime) < timeoutMs) {
              try {
                Thread.sleep(10 * 1000);
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
    context.getDispatcher().getEventHandler().handle(
        new ApplicationMasterEvent(context, ApplicationMasterEventType.FAIL_APP, diag,
            exitCode));
  }

  @Override
  public void stop() {
    isStopped = true;
    LOG.info("stop...");
    while (!taskStore.isNoRunningTask()) {
      LOG.info("have some tasks running so waiting");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        // ignore
      }
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
