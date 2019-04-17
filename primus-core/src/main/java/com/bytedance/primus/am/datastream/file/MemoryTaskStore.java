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

import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.Task;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTaskStore implements TaskStore {

  private static final Logger log = LoggerFactory.getLogger(MemoryTaskStore.class);

  /*
  State machine of tasks:
  pendingTasks -> executorTaskMap (running tasks)
  executorTaskMap -> pendingTasks
               or -> successTasks
               or -> failureTasks
   */
  protected Queue<TaskWrapper> pendingTasks;
  protected Queue<TaskWrapper> successTasks;
  protected Queue<TaskWrapper> failureTasks;
  protected Map<ExecutorId, Map<Long, TaskWrapper>> executorTaskMap;

  private AtomicInteger totalTaskNum;
  private String name;

  private AtomicBoolean hasShuffleCompleted = new AtomicBoolean(false);

  public MemoryTaskStore(String name) {
    this.name = name;
    pendingTasks = new PriorityBlockingQueue<>(10000);
    successTasks = new ConcurrentLinkedQueue<>();
    failureTasks = new ConcurrentLinkedQueue<>();
    executorTaskMap = new ConcurrentHashMap<>();
    totalTaskNum = new AtomicInteger(0);
  }

  @Override
  public void addNewTasks(List<Task> tasks) {
    for (Task task : tasks) {
      pendingTasks.add(new TaskWrapper(name, task));
    }
    totalTaskNum.getAndAdd(tasks.size());
  }

  @Override
  public Task getLastSavedTask() {
    return null;
  }

  @Override
  public int getTotalTaskNum() {
    return totalTaskNum.get();
  }

  @Override
  public void addPendingTasks(TaskWrapper task) {
    pendingTasks.add(task);
  }

  @Override
  public List<TaskWrapper> getPendingTasks(int size) {
    List<TaskWrapper> tasks = new LinkedList<>();
    if (size <= 0) {
      tasks.addAll(pendingTasks);
    } else {
      // Get size tasks at best
      Iterator<TaskWrapper> iterator = pendingTasks.iterator();
      while (size > 0 && iterator.hasNext()) {
        tasks.add(iterator.next());
        size = size - 1;
      }
    }
    return tasks;
  }

  @Override
  public TaskWrapper pollPendingTask() {
    return pendingTasks.poll();
  }

  @Override
  public int getPendingTaskNum() {
    return pendingTasks.size();
  }

  @Override
  public void addSuccessTask(TaskWrapper task) {
    successTasks.add(task);
  }

  @Override
  public Collection<TaskWrapper> getSuccessTasks() {
    return successTasks;
  }

  @Override
  public int getSuccessTaskNum() {
    return successTasks.size();
  }

  @Override
  public void addFailureTask(TaskWrapper task) {
    failureTasks.add(task);
  }

  @Override
  public Collection<TaskWrapper> getFailureTasks() {
    return failureTasks;
  }

  @Override
  public Collection<TaskWrapper> getFailureTasksWithLimit(int limit) {
    if (limit <= 0) {
      return failureTasks;
    }
    return failureTasks.stream().limit(limit).collect(Collectors.toList());
  }

  @Override
  public int getFailureTaskNum() {
    return failureTasks.size();
  }

  @Override
  public void addExecutorRunningTasks(ExecutorId executorId, Map<Long, TaskWrapper> tasks) {
    executorTaskMap.putIfAbsent(executorId, tasks);
  }

  @Override
  public Map<Long, TaskWrapper> getRunningTasks(ExecutorId executorId) {
    return executorTaskMap.get(executorId);
  }

  @Override
  public Map<Long, TaskWrapper> removeExecutorRunningTasks(ExecutorId executorId) {
    return executorTaskMap.remove(executorId);
  }

  @Override
  public Map<ExecutorId, Map<Long, TaskWrapper>> getExecutorRunningTaskMap() {
    return executorTaskMap;
  }

  @Override
  public boolean isNoRunningTask() {
    return executorTaskMap.values().stream().allMatch(t -> t.size() == 0);
  }

  @Override
  public void stop() {
  }

  @Override
  public boolean makeSavepoint(String savepointDir) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean shuffle() {
    if (hasShuffleCompleted.get()) {
      return false;
    }
    List<TaskWrapper> allTasks = new ArrayList<>();
    CollectionUtils.addAll(allTasks, pendingTasks.iterator());
    pendingTasks.clear();
    log.info("Start shuffle tasks, total count:{}", allTasks.size());
    Collections.shuffle(allTasks);
    long defaultTaskId = 1L;
    long minTaskId = allTasks.stream()
        .map(taskWrapper -> taskWrapper.getTask().getTaskId())
        .min(Long::compareTo)
        .orElse(defaultTaskId);
    log.info("shuffle completed, total tasks:{}, minId:{}", allTasks.size(), minTaskId);
    for (TaskWrapper taskWrapper : allTasks) {
      taskWrapper.getTask().setTaskId(minTaskId);
      taskWrapper.getTaskStatus().setTaskId(minTaskId);
      minTaskId++;
      pendingTasks.add(taskWrapper);
    }

    for (Iterator<TaskWrapper> it = pendingTasks.iterator(); it.hasNext(); ) {
      TaskWrapper next = it.next();
      log.info("Shuffled task id:{}", next.getTask().getTaskId());
    }
    return hasShuffleCompleted.compareAndSet(false, true);
  }
}
