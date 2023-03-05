/*
 * Copyright 2023 Bytedance Inc.
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

package com.bytedance.primus.am.datastream.file.task.store.filesystem;

import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_STATE_FAILED_EVENT;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_STATE_SUCCESS_EVENT;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.datastream.file.task.store.TaskStatistics;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.common.util.IntegerUtils;
import com.bytedance.primus.common.util.LockUtils.LockWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaskVault is the in-memory single source of truth for all tasks derived from the corresponding
 * DataSpec. Since task states are critical to application lifecycle, TaskVault is protected with
 * locks to ensure consistency.
 * <p>
 * State machine of tasks
 * <li> 1. newTasks -> newPendingTasks -> executorTaskMap (running tasks) </li>
 * <li> 2. oldPendingTasks (from checkpointed or retried) -> executorTaskMap </li>
 * <li> 3. executorTaskMap -> oldPendingTasks (checkpoint) || successTasks || failureTasks </li>
 */
class TaskVault extends TaskVaultLock {

  private static final Task TASK_DELIMITER = new TaskPBImpl();
  private static final int MIN_PENDING_TASK_NUM = 100000;

  private final Logger LOG;
  private final AMContext context;
  private final String name;

  private final float sampleRate;
  private final Random random = new Random();

  // Task state machines
  private final Queue<Task> newTasks = new ConcurrentLinkedQueue<>();
  private final Queue<Task> newPendingTasks = new PriorityBlockingQueue<>(10000);
  private final Queue<TaskWrapper> oldPendingTasks = new PriorityBlockingQueue<>(1000);
  private final Map<ExecutorId, Map<Long, TaskWrapper>> executorTaskMap = new ConcurrentHashMap<>();
  private final Queue<TaskWrapper> successTasks = new ConcurrentLinkedQueue<>();
  private final Queue<TaskWrapper> failureTasks = new ConcurrentLinkedQueue<>();

  // Statistics
  private final AtomicLong maxLoadedTaskId;
  private final AtomicLong maxSuccessTaskId;
  private final AtomicLong maxFailureTaskId;

  private final AtomicLong totalTaskNum;
  private final AtomicLong successTaskNum;
  private final AtomicLong failureTaskNum;

  public TaskVault(FileSystemTaskStore owner, TaskStorage storage) throws IOException {
    super();

    this.LOG = LoggerFactory.getLogger(TaskVault.class.getName() + "[" + owner.getName() + "]");
    this.context = owner.getContext();
    this.name = owner.getName();
    this.sampleRate = context.getApplicationMeta()
        .getPrimusConf()
        .getInputManager()
        .getFileConfig()
        .getSampleRate();

    try (
        TimerMetric ignored = PrimusMetrics
            .getTimerContextWithAppIdTag("am.taskstore.recover_task.latency")
    ) {
      Checkpoint checkpoint = storage.loadCheckpoint();
      LOG.info("Loaded checkpoint: {}", checkpoint.toLogString());

      maxSuccessTaskId = new AtomicLong(checkpoint.getMaxSuccessTaskId());
      maxFailureTaskId = new AtomicLong(checkpoint.getMaxFailureTaskId());
      maxLoadedTaskId = new AtomicLong(IntegerUtils.maxLong(
          checkpoint.getMaxRunningTaskId(),
          checkpoint.getMaxSuccessTaskId(),
          checkpoint.getMaxFailureTaskId()
      ));

      totalTaskNum = new AtomicLong(storage.getTaskNum());
      successTaskNum = new AtomicLong(checkpoint.getSuccessTaskNum());
      failureTaskNum = new AtomicLong(checkpoint.getFailureTaskNum());

      oldPendingTasks.addAll(storage.loadTaskWrappers(checkpoint.getRunningTaskStatuses()));
    }
  }

  // Task state machine ============================================================================
  // ===============================================================================================

  // Since the task maps of an executor is shared among operations, their existence should be ensured.
  private Map<Long, TaskWrapper> ensureAndGetRunningTaskMap(ExecutorId executorId) {
    return executorTaskMap.computeIfAbsent(executorId, ignored -> new ConcurrentHashMap<>());
  }

  /**
   * Adding new tasks to TaskVault. Locks for driver updates.
   *
   * @param tasks new tasks to add
   */
  public void addNewTasks(List<Task> tasks) {
    if (tasks.isEmpty()) {
      return;
    }
    long taskNum;
    try (LockWrapper ignored = lockForDriverUpdates()) {
      newTasks.addAll(
          new LinkedList<Task>() {{
            addAll(tasks);
            add(TASK_DELIMITER);
          }}
      );
      taskNum = totalTaskNum.addAndGet(tasks.size());
    }
    LOG.info("Added {} new task(s), current totalTaskNum: {}", tasks.size(), taskNum);
  }

  /**
   * Poll a new task from TaskVault. Similar to addNewTasks(), locks for driver updates.
   *
   * @return an optional task, which indicates whether there is a task ready to be served
   */
  public Optional<Task> pollNewTask() {
    try (LockWrapper ignored = lockForDriverUpdates()) {
      Task ret = newTasks.poll();
      return ret == null || ret == TASK_DELIMITER
          ? Optional.empty()
          : Optional.of(ret);
    }
  }

  /**
   * Add a new pending task which is ready to be served to executors, locks for driver updates.
   *
   * @param task to add
   * @return whether the task has been successfully added.
   */
  public boolean addNewPendingTask(Task task) {
    try (LockWrapper ignored = lockForDriverUpdates()) {
      // Prevent pending queue form being polluted
      if (task == null || task == TASK_DELIMITER) {
        return true;
      }

      // Be conservative to reduce memory footprint
      if (oldPendingTasks.size() + newPendingTasks.size() > MIN_PENDING_TASK_NUM) {
        return false;
      }

      // Adding the new task
      newPendingTasks.add(task);
      IntegerUtils.updateAtomicLongIfLarger(
          maxLoadedTaskId,
          task.getTaskId(),
          "maxLoadedTaskId"
      );
      return true;
    }
  }

  /**
   * Update and assign new tasks to an executor. Similar to updateAndRemoveTasks, to allow
   * concurrently updating different executors.
   *
   * @param executorId                ExecutorId
   * @param taskStatusMapFromExecutor task statuses updated from executor
   * @param taskAttemptLimit          the maximum times that a task can fail
   * @param maxTaskNumPerWorker       the maximum number of tasks can be assigned
   * @param needMoreTask              whether the executor accepts more tasks
   * @return the updated assigned tasks for the specified executor
   */
  public Map<Long, TaskWrapper> updateAndAssignTasks(
      ExecutorId executorId,
      Map<Long, TaskStatus> taskStatusMapFromExecutor,
      int taskAttemptLimit,
      int maxTaskNumPerWorker,
      boolean needMoreTask
  ) {
    try (LockWrapper ignored = lockForExecutorUpdates(executorId)) {
      updateCompletedTasks(executorId, taskStatusMapFromExecutor, taskAttemptLimit);
      if (needMoreTask) {
        assignTasks(executorId, maxTaskNumPerWorker);
      }
      return ensureAndGetRunningTaskMap(executorId);
    }
  }

  /**
   * Update and remove tasks assigned to an executor. To allow concurrently updating different
   * executors, this operation locks only an executor.
   *
   * @param executorId                ExecutorId
   * @param taskStatusMapFromExecutor task statuses updated from executor
   * @param taskAttemptLimit          the maximum times that a task can fail
   * @param taskAttemptModifier       the modifier to the attempt count for tasks
   * @return the updated assigned tasks for the specified executor
   */

  public Map<Long, TaskWrapper> updateAndRemoveTasks(
      ExecutorId executorId,
      Map<Long, TaskStatus> taskStatusMapFromExecutor,
      int taskAttemptLimit,
      int taskAttemptModifier
  ) {
    try (LockWrapper ignored = lockForExecutorUpdates(executorId)) {
      updateCompletedTasks(executorId, taskStatusMapFromExecutor, taskAttemptLimit);
      removeTasks(executorId, taskAttemptModifier, taskAttemptLimit);
      return ensureAndGetRunningTaskMap(executorId);
    }
  }


  private void updateCompletedTasks(
      ExecutorId executorId,
      Map<Long, TaskStatus> taskStatusMapFromExecutor,
      int taskAttemptLimit
  ) {
    Map<Long, TaskWrapper> runningTaskMapInDriver = ensureAndGetRunningTaskMap(executorId);

    // Completed tasks
    taskStatusMapFromExecutor.values().forEach(taskStatus -> {
      // Filter astray tasks
      long taskId = taskStatus.getTaskId();
      if (!runningTaskMapInDriver.containsKey(taskId)) {
        LOG.warn("Found an astray task[{}] from executor[{}]", taskId, executorId);
        return;
      }
      // Handle tasks by their states
      switch (taskStatus.getTaskState()) {
        case RUNNING:
          break;
        case SUCCEEDED:
          succeedTask(executorId, taskId);
          break;
        case FAILED:
          failTask(executorId, taskId, taskAttemptLimit);
          break;
        default:
          LOG.warn(
              "Caught a task[{}] of unknown state[{}] from executor[{}]",
              taskStatus.getTaskId(), taskStatus.getTaskState(), executorId
          );
      }
    });
  }

  private void removeTasks(
      ExecutorId executorId,
      int taskAttemptModifier,
      int taskAttemptLimit
  ) {
    ensureAndGetRunningTaskMap(executorId).values().stream()
        .map(taskWrapper -> taskWrapper.adjustNumAttempt(taskAttemptModifier))
        .forEach(taskWrapper -> {
          // Filter tasks by state
          if (TaskState.RUNNING != taskWrapper.getTaskStatus().getTaskState()) {
            LOG.warn("Caught task not in running state from ExecutorTaskMap: {}", taskWrapper);
            return;
          }
          failTask(
              executorId,
              taskWrapper.getTaskStatus().getTaskId(),
              taskAttemptLimit
          );
        });
  }

  private void assignTasks(ExecutorId executorId, int maxTaskNumPerWorker) {
    Map<Long, TaskWrapper> runningTasksInDriver = ensureAndGetRunningTaskMap(executorId);
    if (runningTasksInDriver.size() >= maxTaskNumPerWorker) {
      return;
    }

    pollPendingTask()
        .map(taskWrapper -> taskWrapper.adjustNumAttempt(1))
        .ifPresent(taskWrapper ->
            runningTasksInDriver.put(
                taskWrapper.getTask().getTaskId(),
                taskWrapper
            )
        );
  }

  private Optional<TaskWrapper> pollPendingTask() {
    while (true) {
      TaskWrapper taskWrapper = pollPendingTaskInternal();
      if (taskWrapper == null ||
          taskWrapper.getTaskStatus().getNumAttempt() > 0 || // It's a retry task
          sampleRate == 0 ||                                 // Sampling is not enabled
          sampleRate >= random.nextFloat()                   // Sampled
      ) {
        return Optional.ofNullable(taskWrapper);
      }

      // Skipping the task
      LOG.info("Skipping task, " +
          "taskId[" + taskWrapper.getTaskStatus().getTaskId() + "]." +
          "checkpoint[" + taskWrapper.getTaskStatus().getCheckpoint() + "]." +
          "progress[" + taskWrapper.getTaskStatus().getProgress() + "]." +
          "taskState[" + taskWrapper.getTaskStatus().getTaskState() + "]." +
          "numAttempt[" + taskWrapper.getTaskStatus().getNumAttempt() + "]." +
          "lastAssignTime[" + taskWrapper.getTaskStatus().getLastAssignTime() + "]."
      );

      TaskStatus taskStatus = taskWrapper.getTaskStatus();
      taskStatus.setTaskState(TaskState.SUCCEEDED);
      taskStatus.setDataConsumptionTime(System.currentTimeMillis());
      markAndAddToSuccessTasks(taskWrapper);
    }
  }

  private TaskWrapper pollPendingTaskInternal() {
    TaskWrapper taskWrapper = oldPendingTasks.poll();
    if (taskWrapper != null) {
      return taskWrapper;
    }
    Task task = newPendingTasks.poll();
    if (task != null) {
      return new TaskWrapper(name, task);
    }
    return null;
  }

  private void succeedTask(ExecutorId executorId, long taskId) {
    TaskWrapper succeeded = ensureAndGetRunningTaskMap(executorId).remove(taskId);
    markAndAddToSuccessTasks(succeeded);
    context.logTimelineEvent(
        PRIMUS_TASK_STATE_SUCCESS_EVENT.name(), succeeded.getTaskStatus().toString(),
        "Task[{}] is succeeded, moving from executor[{}] to succeeded tasks.",
        taskId, executorId
    );
  }

  private void markAndAddToSuccessTasks(TaskWrapper task) {
    task.getTaskStatus().setTaskState(TaskState.SUCCEEDED);
    task.getTaskStatus().setFinishTime(new Date().getTime());

    successTasks.add(task);
    successTaskNum.addAndGet(1);
    IntegerUtils.updateAtomicLongIfLarger(
        maxSuccessTaskId,
        task.getTask().getTaskId(),
        "maxSuccessTaskId"
    );
  }

  private void failTask(ExecutorId executorId, long taskId, int maxTaskAttempts) {
    TaskWrapper failed = ensureAndGetRunningTaskMap(executorId).remove(taskId);

    // Try recycling the task
    int attempts = failed.getTask().getNumAttempt();
    if (attempts < maxTaskAttempts) {
      LOG.info(
          "Task[{}] has failed for {} time(s), moving from executor[{}] back to pending tasks.",
          taskId, attempts, executorId
      );
      oldPendingTasks.add(failed);
      // TODO: Add a new timeline event for recycling a failed task
      return;
    }

    // Fail the task
    LOG.warn(
        "Task[{}] has reached maxTaskAttempts({}), moving from executor[{}] to failed tasks.",
        taskId, maxTaskAttempts, executorId
    );
    markAndAddToFailureTasks(failed);
    context.logTimelineEvent(
        PRIMUS_TASK_STATE_FAILED_EVENT.name(), failed.getTask().toString(),
        "Task[{}] is succeeded, moving from executor[{}] to succeeded tasks.",
        taskId, executorId
    );
  }

  private void markAndAddToFailureTasks(TaskWrapper task) {
    task.getTaskStatus().setTaskState(TaskState.FAILED);
    task.getTaskStatus().setFinishTime(new Date().getTime());

    failureTasks.add(task);
    failureTaskNum.addAndGet(1);
    IntegerUtils.updateAtomicLongIfLarger(
        maxFailureTaskId,
        task.getTask().getTaskId(),
        "maxFailureTaskId"
    );
  }

  // Task statistics ===============================================================================
  // ===============================================================================================

  // LockAll for accuracy
  public boolean hasBeenExhausted() {
    try (LockWrapper ignored = lockAll()) {
      return maxLoadedTaskId.get() + 1 == totalTaskNum.get() // all tasks have been loaded
          && oldPendingTasks.isEmpty() // no old pending task
          && newPendingTasks.isEmpty() // no new pending task
          && !hasRunningTask();        // no running task
    }
  }

  // LockAll for accuracy
  public boolean hasRunningTask() {
    try (LockWrapper ignored = lockAll()) {
      return executorTaskMap.values().stream().anyMatch(tasks -> tasks.size() > 0);
    }
  }

  // LockAll for accuracy
  public TaskStatistics getTaskStatistics() {
    try (LockWrapper ignored = lockAll()) {
      long totalNum = totalTaskNum.get();
      long runningNum = executorTaskMap.values().stream().map(Map::size).reduce(0, Integer::sum);
      long successNum = successTaskNum.get();
      long failureNum = failureTaskNum.get();
      return new TaskStatistics(
          totalNum,
          totalNum - runningNum - successNum - failureNum,
          runningNum,
          successNum,
          failureNum
      );
    }
  }

  public long getMaxLoadedTaskId() {
    return maxLoadedTaskId.get();
  }

  public long getMaxSuccessTaskId() {
    return maxSuccessTaskId.get();
  }

  public long getMaxFailureTaskId() {
    return maxFailureTaskId.get();
  }

  // LockAll for accuracy
  public List<TaskWrapper> peekPendingTasks(int limit) {
    if (limit <= 0) {
      return new ArrayList<>();
    }

    try (LockWrapper ignored = lockAll()) {
      PrimusMetrics.emitStoreWithAppIdTag(
          "am.taskstore.memory_pending_task_num",
          oldPendingTasks.size() + newPendingTasks.size()
      );

      // Start collecting tasks
      List<TaskWrapper> tasks = new LinkedList<>();
      Iterator<TaskWrapper> taskWrapperIterator = oldPendingTasks.iterator();
      while (taskWrapperIterator.hasNext() && limit-- > 0) {
        tasks.add(taskWrapperIterator.next());
      }

      Iterator<Task> taskIterator = newPendingTasks.iterator();
      while (taskIterator.hasNext() && limit-- > 0) {
        tasks.add(new TaskWrapper(name, taskIterator.next()));
      }

      return tasks;
    }
  }

  // LockAll for accuracy
  public Map<Long, TaskWrapper> peekRunningTasks() {
    try (LockWrapper ignored = lockAll()) {
      return executorTaskMap.values().stream().reduce(
          new HashMap<>(),
          (acc, current) -> {
            acc.putAll(current);
            return acc;
          });
    }
  }

  // LockAll for accuracy
  public Collection<TaskWrapper> peekSuccessTasks(int limit) {
    if (limit <= 0) {
      return new ArrayList<>();
    }
    try (LockWrapper ignored = lockAll()) {
      return snapshotTasksFromQueue(successTasks, limit);
    }
  }

  // LockAll for accuracy
  public Collection<TaskWrapper> peekFailureTasks(int limit) {
    if (limit <= 0) {
      return new ArrayList<>();
    }
    try (LockWrapper ignored = lockAll()) {
      return snapshotTasksFromQueue(failureTasks, limit);
    }
  }

  private static Collection<TaskWrapper> snapshotTasksFromQueue(
      Queue<TaskWrapper> queue,
      int limit
  ) {
    List<TaskWrapper> tasks = new LinkedList<>();
    Iterator<TaskWrapper> taskWrapperIterator = queue.iterator();
    while (taskWrapperIterator.hasNext() && limit-- > 0) {
      tasks.add(taskWrapperIterator.next());
    }
    return tasks;
  }

  // Checkpoint utils ==============================================================================
  // ===============================================================================================

  public Checkpoint generateCheckpoint(float progress) {
    // Serialized and make a copy in memory to avoid big critical sections
    try (LockWrapper ignored = lockAll()) {
      // Copy and serialize running tasks without mutation, since their states aren't final.
      List<TaskStatus> runningTasks = new LinkedList<TaskStatus>() {{
        addAll(oldPendingTasks.stream()
            .map(TaskWrapper::getTaskStatus)
            .collect(Collectors.toList()));
        addAll(executorTaskMap.values().stream()
            .flatMap(p -> p.values().stream())
            .map(TaskWrapper::getTaskStatus)
            .collect(Collectors.toList()));
      }};

      long maxRunningTaskId = runningTasks.stream()
          .map(TaskStatus::getTaskId)
          .reduce(-1L, Math::max);

      List<byte[]> serializedRunningTasks = runningTasks.stream()
          .map(TaskStatus::toByteArray)
          .collect(Collectors.toList());

      // Removes and serialize completed tasks from their input queue to reduce memory footprint.
      List<byte[]> serializedSuccessTasks = pollAndSerializeTasks(successTasks);
      List<byte[]> serializedFailureTasks = pollAndSerializeTasks(failureTasks);

      // Assemble result
      return Checkpoint.newCheckpointToPersist(
          progress,
          successTaskNum.get(),
          failureTaskNum.get(),
          maxRunningTaskId,
          maxSuccessTaskId.get(),
          maxFailureTaskId.get(),
          serializedRunningTasks,
          serializedSuccessTasks,
          serializedFailureTasks
      );
    }
  }

  private static List<byte[]> pollAndSerializeTasks(Queue<TaskWrapper> queue) {
    List<byte[]> results = new LinkedList<>();
    while (!queue.isEmpty()) {
      // Remove tasks by poll() to avoid OOM
      TaskWrapper taskWrapper = queue.poll();
      results.add(taskWrapper.getTaskStatus().toByteArray());
    }
    return results;
  }
}
