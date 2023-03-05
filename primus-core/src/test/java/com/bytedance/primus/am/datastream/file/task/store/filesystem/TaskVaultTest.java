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

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.datastream.file.task.store.TaskStatistics;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.proto.PrimusCommon.Version;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TaskVaultTest extends TaskStoreTestCommon {

  @Test
  public void testNewTaskVault() throws IOException {
    // Init
    AMContext mockedAMContext = newMockedAMContext();
    TaskVault taskVault = new TaskVault(
        newMockedFileSystemTaskStore(mockedAMContext),
        newMockedTaskStorage()
    );

    // Verify checkpoint
    Checkpoint checkpoint = taskVault.generateCheckpoint(0);
    Assertions.assertEquals(checkpoint.getVersion(), Version.V2);
    Assertions.assertEquals(checkpoint.getProgress(), 0.0);
    Assertions.assertEquals(checkpoint.getMaxRunningTaskId(), -1);
    Assertions.assertEquals(checkpoint.getSuccessTaskNum(), 0);
    Assertions.assertEquals(checkpoint.getMaxSuccessTaskId(), -1);
    Assertions.assertEquals(checkpoint.getFailureTaskNum(), 0);
    Assertions.assertEquals(checkpoint.getMaxSuccessTaskId(), -1);
  }

  @Test
  public void testUpdateAndAssignWithSuccessTasks() throws IOException {
    int totalTaskNum = 8;

    ExecutorId executorId = newExecutorId();
    TaskVault taskVault = prepareTaskVaultWithPendingTasks(totalTaskNum);
    Assertions.assertFalse(taskVault.hasRunningTask());

    Map<Long, TaskWrapper> assignedTaskWrappers = new HashMap<>();
    for (int i = 0; i <= totalTaskNum; ++i) {
      assignedTaskWrappers = taskVault.updateAndAssignTasks(
          executorId,
          succeedTasks(assignedTaskWrappers), // taskStatusMapFromExecutor
          3,   // taskAttemptLimit
          2,   // maxTaskNumPerWorker
          true // needMoreTask
      );

      Assertions.assertEquals(totalTaskNum - 1, taskVault.getMaxLoadedTaskId());
      Assertions.assertEquals(i - 1, taskVault.getMaxSuccessTaskId());
      Assertions.assertEquals(-1, taskVault.getMaxFailureTaskId());

      boolean isFinalIteration = (i == totalTaskNum);
      Assertions.assertEquals(isFinalIteration, !taskVault.hasRunningTask());
      Assertions.assertEquals(isFinalIteration, assignedTaskWrappers.isEmpty());
      Assertions.assertEquals(
          new TaskStatistics(
              totalTaskNum,                                  // TotalTaskNum
              isFinalIteration ? 0 : (totalTaskNum - i - 1), // PendingTaskNum
              isFinalIteration ? 0 : 1,                      // RunningTaskNum
              i, // SuccessTaskNum
              0  // FailureTaskNum
          ),
          taskVault.getTaskStatistics()
      );
    }
  }

  @Test
  public void testUpdateAndAssignWithFailureTasks() throws IOException {
    int totalTaskNum = 8;
    int taskAttemptLimit = 3;

    ExecutorId executorId = newExecutorId();
    TaskVault taskVault = prepareTaskVaultWithPendingTasks(totalTaskNum);
    Assertions.assertFalse(taskVault.hasRunningTask());

    Map<Long, TaskWrapper> assignedTaskWrappers = new HashMap<>();
    for (int i = 0; i <= totalTaskNum; ++i) {
      for (int j = 0; j < taskAttemptLimit; ++j) {
        assignedTaskWrappers = taskVault.updateAndAssignTasks(
            executorId,
            failTasks(assignedTaskWrappers), // taskStatusMapFromExecutor
            taskAttemptLimit,                // taskAttemptLimit
            2,   // maxTaskNumPerWorker
            true // needMoreTask
        );

        Assertions.assertEquals(totalTaskNum - 1, taskVault.getMaxLoadedTaskId());
        Assertions.assertEquals(-1, taskVault.getMaxSuccessTaskId());
        Assertions.assertEquals(i - 1, taskVault.getMaxFailureTaskId());

        boolean isFinalIteration = (i == totalTaskNum);
        Assertions.assertEquals(isFinalIteration, !taskVault.hasRunningTask());
        Assertions.assertEquals(isFinalIteration, assignedTaskWrappers.isEmpty());
        Assertions.assertEquals(
            new TaskStatistics(
                totalTaskNum,                                  // TotalTaskNum
                isFinalIteration ? 0 : (totalTaskNum - i - 1), // PendingTaskNum
                isFinalIteration ? 0 : 1,                      // RunningTaskNum
                0, // SuccessTaskNum
                i  // FailureTaskNum
            ),
            taskVault.getTaskStatistics()
        );
      }
    }
  }

  @Test
  public void testUpdateAndAssignWithRemovedTasks() throws IOException {
    int totalTaskNum = 4;
    int taskAttemptLimit = 3;
    List<Integer> taskAttemptModifiers = new ArrayList<Integer>() {{
      add(-1);
      add(0);
      add(0);
      add(0);
    }};

    ExecutorId executorId = newExecutorId();
    TaskVault taskVault = prepareTaskVaultWithPendingTasks(totalTaskNum);
    Assertions.assertFalse(taskVault.hasRunningTask());

    // Iterate through the tasks with an additional iteration for checking final status
    for (int i = 0; i <= totalTaskNum; ++i) {
      // Iterate through attempt modifier, where the task is failed in the last inner iteration.
      for (int j = 0; j < taskAttemptModifiers.size(); ++j) {
        // Assign new tasks
        Map<Long, TaskWrapper> assignedTaskWrappers = taskVault.updateAndAssignTasks(
            executorId,
            new HashMap<>(), // taskStatusMapFromExecutor
            taskAttemptLimit,
            2,               // maxTaskNumPerWorker
            true             // needMoreTask
        );

        Assertions.assertEquals(totalTaskNum - 1, taskVault.getMaxLoadedTaskId());
        Assertions.assertEquals(-1, taskVault.getMaxSuccessTaskId());
        Assertions.assertEquals(i - 1, taskVault.getMaxFailureTaskId());

        boolean isFinalOuterIteration = (i == totalTaskNum);
        Assertions.assertEquals(isFinalOuterIteration, !taskVault.hasRunningTask());
        Assertions.assertEquals(isFinalOuterIteration, assignedTaskWrappers.isEmpty());
        Assertions.assertEquals(
            new TaskStatistics(
                totalTaskNum,                                       // TotalTaskNum
                isFinalOuterIteration ? 0 : (totalTaskNum - i - 1), // PendingTaskNum
                isFinalOuterIteration ? 0 : 1,                      // RunningTaskNum
                0, // SuccessTaskNum
                i  // FailureTaskNum
            ),
            taskVault.getTaskStatistics()
        );

        // Remove all tasks with attemptModifier
        assignedTaskWrappers = taskVault.updateAndRemoveTasks(
            executorId,
            extractTaskStatuses(assignedTaskWrappers),
            taskAttemptLimit,
            taskAttemptModifiers.get(j)
        );

        boolean hasFailedTask = !isFinalOuterIteration && (j + 1 == taskAttemptModifiers.size());
        Assertions.assertEquals(totalTaskNum - 1, taskVault.getMaxLoadedTaskId());
        Assertions.assertEquals(-1, taskVault.getMaxSuccessTaskId());
        Assertions.assertEquals(hasFailedTask ? i : i - 1, taskVault.getMaxFailureTaskId());

        long failureTaskNum = i + (hasFailedTask ? 1 : 0);
        Assertions.assertFalse(taskVault.hasRunningTask());
        Assertions.assertTrue(assignedTaskWrappers.isEmpty());
        Assertions.assertEquals(
            new TaskStatistics(
                totalTaskNum,                  // TotalTaskNum
                totalTaskNum - failureTaskNum, // PendingTaskNum
                0,                             // RunningTaskNum
                0,                             // SuccessTaskNum
                failureTaskNum
            ),
            taskVault.getTaskStatistics()
        );
      }
    }
  }

  private TaskVault prepareTaskVaultWithPendingTasks(int size) throws IOException {
    // Init
    AMContext mockedAMContext = newMockedAMContext();
    TaskVault taskVault = new TaskVault(
        newMockedFileSystemTaskStore(mockedAMContext),
        newMockedTaskStorage()
    );

    Assertions.assertEquals(-1, taskVault.getMaxLoadedTaskId());
    Assertions.assertEquals(-1, taskVault.getMaxSuccessTaskId());
    Assertions.assertEquals(-1, taskVault.getMaxFailureTaskId());
    Assertions.assertEquals(
        new TaskStatistics(
            0, // TotalTaskNum
            0, // PendingTaskNum
            0,  // RunningTaskNum
            0,  // SuccessTaskNum
            0   // FailureTaskNum
        ),
        taskVault.getTaskStatistics()
    );

    // Add new Tasks
    List<Task> newTasks = newTaskList(0, size);
    taskVault.addNewTasks(newTasks);

    Assertions.assertEquals(-1, taskVault.getMaxLoadedTaskId());
    Assertions.assertEquals(-1, taskVault.getMaxSuccessTaskId());
    Assertions.assertEquals(-1, taskVault.getMaxFailureTaskId());
    Assertions.assertEquals(
        new TaskStatistics(
            size, // TotalTaskNum
            size, // PendingTaskNum
            0,  // RunningTaskNum
            0,  // SuccessTaskNum
            0   // FailureTaskNum
        ),
        taskVault.getTaskStatistics()
    );

    // Poll new tasks
    List<Task> polled = new ArrayList<>();
    for (Optional<Task> t = taskVault.pollNewTask(); t.isPresent(); t = taskVault.pollNewTask()) {
      polled.add(t.get());
    }
    Assertions.assertEquals(newTasks, polled);

    Assertions.assertEquals(-1, taskVault.getMaxLoadedTaskId());
    Assertions.assertEquals(-1, taskVault.getMaxSuccessTaskId());
    Assertions.assertEquals(-1, taskVault.getMaxFailureTaskId());
    Assertions.assertEquals(
        new TaskStatistics(
            size, // TotalTaskNum
            size, // PendingTaskNum
            0,  // RunningTaskNum
            0,  // SuccessTaskNum
            0   // FailureTaskNum
        ),
        taskVault.getTaskStatistics()
    );

    // Add to new pending tasks
    for (int i = 0; i < polled.size(); ++i) {
      taskVault.addNewPendingTask(polled.get(i));

      Assertions.assertEquals(i, taskVault.getMaxLoadedTaskId());
      Assertions.assertEquals(-1, taskVault.getMaxSuccessTaskId());
      Assertions.assertEquals(-1, taskVault.getMaxFailureTaskId());
      Assertions.assertEquals(
          new TaskStatistics(
              size, // TotalTaskNum
              size, // PendingTaskNum
              0,  // RunningTaskNum
              0,  // SuccessTaskNum
              0   // FailureTaskNum
          ),
          taskVault.getTaskStatistics()
      );
    }

    return taskVault;
  }

  private static TaskStorage newMockedTaskStorage() throws IOException {
    TaskStorage mockedTaskStorage = Mockito.mock(TaskStorage.class);
    Mockito
        .when(mockedTaskStorage.loadCheckpoint())
        .thenReturn(new Checkpoint());

    return mockedTaskStorage;
  }

  private static ExecutorId newExecutorId() {
    ExecutorId executorId = new ExecutorIdPBImpl();
    executorId.setRoleName(String.valueOf(0));
    executorId.setIndex(0);
    executorId.setUniqId(System.currentTimeMillis());
    return executorId;
  }

  private static Map<Long, TaskStatus> extractTaskStatuses(
      Map<Long, TaskWrapper> assignedTaskWrappers
  ) {
    return assignedTaskWrappers.values().stream()
        .map(TaskWrapper::getTaskStatus)
        .collect(Collectors.toMap(
            TaskStatus::getTaskId,
            status -> status)
        );
  }

  private static Map<Long, TaskStatus> succeedTasks(Map<Long, TaskWrapper> assignedTaskWrappers) {
    return assignedTaskWrappers.values().stream()
        .map(taskWrapper -> {
              TaskStatus status = taskWrapper.getTaskStatus();
              status.setTaskState(TaskState.SUCCEEDED);
              return status;
            }
        ).collect(Collectors.toMap(
            TaskStatus::getTaskId,
            status -> status)
        );
  }

  private static Map<Long, TaskStatus> failTasks(Map<Long, TaskWrapper> assignedTaskWrappers) {
    return assignedTaskWrappers.values().stream()
        .map(taskWrapper -> {
              TaskStatus status = taskWrapper.getTaskStatus();
              status.setTaskState(TaskState.FAILED);
              return status;
            }
        ).collect(Collectors.toMap(
            TaskStatus::getTaskId,
            status -> status)
        );
  }
}
