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

package com.bytedance.primus.executor.task;

import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.common.child.ChildLauncherEvent;
import com.bytedance.primus.common.child.ChildLauncherEventType;
import com.bytedance.primus.common.event.Dispatcher;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.service.AbstractService;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.task.file.CommonFileTaskRunner;
import com.bytedance.primus.executor.task.kafka.KafkaTaskRunner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskRunnerManager extends AbstractService implements
    EventHandler<TaskRunnerManagerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(TaskRunnerManager.class);

  private ExecutorContext executorContext;
  private Dispatcher dispatcher;
  private WorkerFeeder workerFeeder;
  private Map<String, TaskRunner> taskRunnerMap;
  private volatile boolean isStopped = false;
  private Map<String, TaskRunner> removedTaskMap;

  public TaskRunnerManager(ExecutorContext context, Dispatcher dispatcher) {
    super(TaskRunnerManager.class.getName());
    this.executorContext = context;
    this.dispatcher = dispatcher;
    this.workerFeeder = executorContext.getWorkerFeeder();
    this.taskRunnerMap = new ConcurrentHashMap<>();
    this.removedTaskMap = new ConcurrentHashMap<>();
  }

  @Override
  public void handle(TaskRunnerManagerEvent taskRunnerEvent) {
    switch (taskRunnerEvent.getType()) {
      case TASK_ASSIGN:
        TaskAssignEvent taskAssignEvent = (TaskAssignEvent) taskRunnerEvent;
        Task task = taskAssignEvent.getTask();
        LOG.info("Assign task[" + task.getUid() + "]");
        if (!taskRunnerMap.containsKey(task.getUid())) {
          try {
            TaskRunner taskRunner = createTaskRunner(task);
            taskRunnerMap.put(task.getUid(), taskRunner);
            // taskRunner.init();
            taskRunner.startTaskRunner();
            LOG.info("Start task runner for task[" + task.getUid() + "]");
          } catch (Exception e) {
            LOG.error("Task runner start failed, task[" + task.getUid() + "]", e);
          }
        } else {
          LOG.info("Task[" + task.getUid() + "] is running, do not need new runner");
        }
        break;
      case TASK_REMOVE:
        TaskRemoveEvent taskRemoveEvent = (TaskRemoveEvent) taskRunnerEvent;
        removeTask(taskRemoveEvent.getTaskUid());
        break;
      case TASK_REMOVE_ALL:
        if (taskRunnerMap.isEmpty()) {
          dispatcher.getEventHandler().handle(new ChildLauncherEvent(ChildLauncherEventType.STOP));
        }
        for (String taskUid : taskRunnerMap.keySet()) {
          removeTask(taskUid);
        }
        isStopped = true;
        break;
      case TASK_REMOVED:
        TaskRemovedEvent taskRemovedEvent = (TaskRemovedEvent) taskRunnerEvent;
        String taskUid = taskRemovedEvent.getTaskUid();
        if (taskRunnerMap.containsKey(taskUid)) {
          LOG.info("Add task[" + taskUid + "] to removed tasks");
          removedTaskMap.put(taskUid, taskRunnerMap.remove(taskUid));
        }
        if (isStopped && taskRunnerMap.isEmpty()) {
          LOG.info("All task runner removed, stopping worker");
          dispatcher.getEventHandler().handle(new ChildLauncherEvent(ChildLauncherEventType.STOP));
        }
        break;
    }
  }

  private void removeTask(String taskUid) {
    if (taskRunnerMap.containsKey(taskUid)) {
      LOG.info("Stop task runner for task[" + taskUid + "]");
      TaskRunner taskRunner = taskRunnerMap.get(taskUid);
      taskRunner.stopTaskRunner();
    }
  }

  public List<TaskStatus> getTaskStatusList() {
    List<TaskStatus> taskStatusList = new ArrayList<>();
    for (TaskRunner taskRunner : taskRunnerMap.values()) {
      taskStatusList.add(taskRunner.getTaskStatus());
    }
    Iterator<Map.Entry<String, TaskRunner>> it = removedTaskMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, TaskRunner> entry = it.next();
      TaskRunner taskRunner = entry.getValue();
      taskStatusList.add(taskRunner.getTaskStatus());
      LOG.info("Remove task[" + entry.getKey() + "] in removed tasks");
      it.remove();
    }
    return taskStatusList;
  }

  private TaskRunner createTaskRunner(Task task) throws IOException {
    switch (task.getTaskType()) {
      case KAFKA_TASK:
        return new KafkaTaskRunner(task, executorContext, workerFeeder);
      case SPLIT_TASK:
        return new CommonFileTaskRunner(task, executorContext, workerFeeder);
      default:
        throw new IOException("Unsupported task type: " + task.getTaskType());
    }
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }
}
