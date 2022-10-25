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
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusInput.WorkPreserve;
import com.bytedance.primus.proto.PrimusInput.WorkPreserve.HdfsConfig;
import com.bytedance.primus.utils.FSDataInputStreamUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonObject;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTaskStore implements TaskStore {

  private static final Task TASK_DELIMITER = new TaskPBImpl();
  private static final int MIN_PENDING_TASK_NUM = 100000;
  private static final int THREAD_SLEEP_MS = 3000;
  private static final int RETAIN_SNAPSHOT_NUMBER = 10;

  private static final String TASKS_META_FILENAME = "tasks.meta";
  private static final String TASKS_INDEX_FILENAME = "tasks.index";
  private static final String TASKS_FILENAME = "tasks";
  private static final String SUCCESS_TASK_STATUSES_FILENAME = "success_task_statuses";
  private static final String FAILURE_TASK_STATUSES_FILENAME = "failure_task_statuses";
  private static final String RUNNING_TASK_STATUSES_FILENAME = "running_task_statuses";
  private static final String SNAPSHOT_FILENAME = "snapshot";

  /*
  Task id starts with 1.

  State machine of tasks:
  1. newTasks -> newPendingTasks -> executorTaskMap (running tasks)
  2. oldPendingTasks -> executorTaskMap
  3. executorTaskMap -> oldPendingTasks
                  or -> successTasks
                  or -> failureTasks

   (1) oldPendingTasks has a high priority than newPendingTasks.
   (2) Tasks in oldPendingTasks, successTasks and failureTasks have been executed and
   their states must be stored in a persistent storage (file system).
   */
  private Logger LOG;
  private AMContext context;
  private String name;
  private ReentrantReadWriteLock readWriteLock;
  private Queue<Task> newTasks;
  private Queue<Task> newPendingTasks;
  private Queue<TaskWrapper> oldPendingTasks;
  private Queue<TaskWrapper> successTasks;
  private Queue<TaskWrapper> failureTasks;
  private Map<ExecutorId, Map<Long, TaskWrapper>> executorTaskMap;

  private CompressionCodec compressionCodec;
  private int dumpIntervalSeconds;

  /*---------- Old states ----------*/
  private Task lastSavedTask;  // Set only once at initialization or recovery
  private AtomicLong currentSavedTaskId;  // Control slow start of TaskLoader after TaskSaver
  private AtomicLong totalTaskNum;
  private AtomicLong successTaskNum;
  private AtomicLong maxSuccessTaskId;
  private AtomicLong failureTaskNum;
  private AtomicLong maxFailureTaskId;
  private volatile long nextTaskIdToLoad;
  /*---------- Old states ----------*/

  private String storeDir;
  private Map<Long, Integer> taskIdFileIdMap;  // Records the first task id of a tasks file
  private Path tasksMetaPath;
  private Path tasksIndexPath;  // Store taskIdFileIdMap
  private Path tasksPath;
  private Path successTaskStatusesPath;
  private Path failureTaskStatusesPath;
  private Path runningTaskStatusesPath;
  private ReentrantReadWriteLock taskFileLock;  // lock for tasks* file, not including running/success/failure task
  private Path snapshotDir;
  private final FileSystem fs;
  private int copyThreadCnt;
  private volatile boolean isStopped = false;
  private TaskSaver taskSaver;
  private TaskLoader taskLoader;
  private TaskPreserver taskPreserver;

  public FileTaskStore(AMContext context, String name, String savepointDir,
      ReentrantReadWriteLock readWriteLock)
      throws IOException {
    this.context = context;
    this.name = name;
    this.LOG = LoggerFactory.getLogger(FileTaskStore.class.getName() + "[" + name + "]");
    this.readWriteLock = readWriteLock;
    newTasks = new ConcurrentLinkedQueue<>();
    oldPendingTasks = new PriorityBlockingQueue<>(1000);
    newPendingTasks = new PriorityBlockingQueue<>(10000);
    successTasks = new ConcurrentLinkedQueue<>();
    failureTasks = new ConcurrentLinkedQueue<>();
    executorTaskMap = new ConcurrentHashMap<>();
    compressionCodec = getCompressionCodec();
    PrimusConf primusConf = context.getPrimusConf();
    dumpIntervalSeconds = Math.max(5000,
        primusConf.getInputManager().getWorkPreserve().getDumpIntervalSecs());
    copyThreadCnt =
        primusConf.getSnapshotCopyThreadsCnt() == 0 ? 4 : primusConf.getSnapshotCopyThreadsCnt();
    lastSavedTask = null;
    currentSavedTaskId = new AtomicLong(0);
    totalTaskNum = new AtomicLong(0);
    successTaskNum = new AtomicLong(0);
    maxSuccessTaskId = new AtomicLong(0);
    failureTaskNum = new AtomicLong(0);
    maxFailureTaskId = new AtomicLong(0);
    nextTaskIdToLoad = 1;

    taskIdFileIdMap = new TreeMap<>();
    storeDir = getStoreDir(context.getApplicationNameForStorage(), name, primusConf);
    tasksMetaPath = new Path(storeDir, TASKS_META_FILENAME);
    tasksIndexPath = new Path(storeDir, TASKS_INDEX_FILENAME);
    tasksPath = new Path(storeDir, TASKS_FILENAME);
    successTaskStatusesPath = new Path(storeDir, SUCCESS_TASK_STATUSES_FILENAME);
    failureTaskStatusesPath = new Path(storeDir, FAILURE_TASK_STATUSES_FILENAME);
    runningTaskStatusesPath = new Path(storeDir, RUNNING_TASK_STATUSES_FILENAME);
    taskFileLock = new ReentrantReadWriteLock();
    snapshotDir = new Path(storeDir, SNAPSHOT_FILENAME);

    fs = context.getHadoopFileSystem();
    taskSaver = new TaskSaver();
    taskLoader = new TaskLoader();
    taskPreserver = new TaskPreserver();

    Path storePath = new Path(storeDir);
    Path oldStorePath = StringUtils.isEmpty(savepointDir) ? storePath : new Path(savepointDir);
    boolean oldStorePathExist = fs.exists(oldStorePath);
    int oldStoreFileCount = oldStorePathExist ? fs.listStatus(oldStorePath).length : 0;
    LOG.info("OldStorePath " + oldStorePath + ", exist " + oldStorePathExist + ", fileCount "
        + oldStoreFileCount);
    if (oldStorePathExist && oldStoreFileCount != 0) {
      LOG.info("Recovering states...");
      TimerMetric recoverLatency = PrimusMetrics
          .getTimerContextWithOptionalPrefix("am.taskstore.recover.cost{name=" + name + "}");
      long latency = System.currentTimeMillis();
      if (oldStorePath != storePath) {
        LOG.info("Delete all checkpoint files under storePath:{}", storePath);
        fs.delete(runningTaskStatusesPath, true);
        fs.delete(new Path(runningTaskStatusesPath + ".tmp"), true);
        fs.delete(new Path(runningTaskStatusesPath + ".tmp.lock"), true);
        fs.delete(successTaskStatusesPath, true);
        fs.delete(failureTaskStatusesPath, true);

        deleteTaskMetaFiles(storePath);

        String filesPrefix = oldStorePath + "/" + name + "/*";
        if (!copyFilesParallel(filesPrefix, storePath, copyThreadCnt)) {
          throw new IOException("Failed to copy files from " + filesPrefix + " to " + storePath);
        }
      }
      taskSaver.recover();
      taskPreserver.recover();
      latency = System.currentTimeMillis() - latency;
      recoverLatency.stop();
      LOG.info("Recovery elapsed " + latency + " ms");
      LOG.info("lastSavedTask: " + lastSavedTask);
      LOG.info("currentSaveTaskId " + currentSavedTaskId + ", totalTaskNum " + totalTaskNum
          + ", nextTaskIdToLoad " + nextTaskIdToLoad
          + ", successTaskNum " + successTaskNum + ", maxSuccessTaskId " + maxSuccessTaskId
          + ", failureTaskNum " + failureTaskNum + ", maxFailureTaskId " + maxFailureTaskId
          + ", lastRunningTaskNum " + oldPendingTasks.size());
    }

    taskSaver.start();
    taskLoader.start();
    taskPreserver.start();
  }

  private void deleteTaskMetaFiles(Path storeDir) throws IOException {
    Path taskMetaFiles = new Path(storeDir, TASKS_FILENAME + ".*");
    FileStatus[] fileStatuses = fs.globStatus(taskMetaFiles);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isFile()) {
        boolean isDeleted = fs.delete(fileStatus.getPath(), false);
        if (!isDeleted) {
          LOG.error("failed to delete task meta file:{}", fileStatus.getPath());
        } else {
          LOG.info("deleted task meta file:{}", fileStatus.getPath());
        }
      }
    }
  }

  @Override
  public void addNewTasks(List<Task> tasks) {
    newTasks.addAll(tasks);
    newTasks.add(TASK_DELIMITER);
    totalTaskNum.getAndAdd(tasks.size());
    LOG.info("Add " + tasks.size() + " new tasks, current totalTaskNum " + totalTaskNum.get());
  }

  @Override
  public Task getLastSavedTask() {
    return lastSavedTask;
  }

  @Override
  public int getTotalTaskNum() {
    return (int) totalTaskNum.get();
  }

  @Override
  public void addPendingTasks(TaskWrapper task) {
    oldPendingTasks.add(task);
  }

  @Override
  public List<TaskWrapper> getPendingTasks(int size) {
    int pendingTaskNumInMemory = oldPendingTasks.size() + newPendingTasks.size();
    PrimusMetrics.emitCounterWithOptionalPrefix("am.taskstore.memory_pending_task_num",
        pendingTaskNumInMemory);
    List<TaskWrapper> tasks = new LinkedList<>();
    if (size <= 0) {
      throw new UnsupportedOperationException(FileTaskStore.class.getSimpleName()
          + " do not support getting all pending tasks");
    } else {
      // Get size tasks at best
      Iterator<TaskWrapper> iterator = oldPendingTasks.iterator();
      while (size > 0 && iterator.hasNext()) {
        tasks.add(iterator.next());
        size = size - 1;
      }
      Iterator<Task> taskIterator = newPendingTasks.iterator();
      while (size > 0 && taskIterator.hasNext()) {
        tasks.add(new TaskWrapper(name, taskIterator.next()));
        size = size - 1;
      }
    }
    return tasks;
  }

  @Override
  public TaskWrapper pollPendingTask() {
    TaskWrapper taskWrapper = oldPendingTasks.poll();
    if (taskWrapper == null) {
      Task task = newPendingTasks.poll();
      if (task != null) {
        taskWrapper = new TaskWrapper(name, task);
      }
    }
    return taskWrapper;
  }

  @Override
  public int getPendingTaskNum() {
    int tasksToLoad = (int) (totalTaskNum.get() - nextTaskIdToLoad + 1);
    int tasksLoaded = oldPendingTasks.size() + newPendingTasks.size();
    return tasksToLoad > 0 ? (tasksLoaded + tasksToLoad) : tasksLoaded;
  }

  @Override
  public synchronized void addSuccessTask(TaskWrapper task) {
    context.logStatusEvent(context.getStatusEventWrapper().buildTaskStatusEvent(task));
    successTasks.add(task);
    successTaskNum.getAndAdd(1);
    if (task.getTask().getTaskId() > maxSuccessTaskId.get()) {
      maxSuccessTaskId.set(task.getTask().getTaskId());
    }
  }

  @Override
  public Collection<TaskWrapper> getSuccessTasks() {
    return successTasks;
  }

  @Override
  public int getSuccessTaskNum() {
    return (int) successTaskNum.get();
  }

  @Override
  public synchronized void addFailureTask(TaskWrapper task) {
    context.logStatusEvent(context.getStatusEventWrapper().buildTaskStatusEvent(task));
    failureTasks.add(task);
    failureTaskNum.getAndAdd(1);
    if (task.getTask().getTaskId() > maxFailureTaskId.get()) {
      maxFailureTaskId.set(task.getTask().getTaskId());
    }
  }

  @Override
  public Collection<TaskWrapper> getFailureTasks() {
    List<TaskWrapper> tasks =
        getFailureTasksFromFs(
            context.getPrimusConf(),
            -1);
    tasks.addAll(failureTasks);
    return tasks;
  }

  @Override
  public Collection<TaskWrapper> getFailureTasksWithLimit(int limit) {
    List<TaskWrapper> tasks =
        getFailureTasksFromFs(
            context.getPrimusConf(),
            limit
        );
    tasks.addAll(failureTasks);
    return tasks;
  }

  @Override
  public int getFailureTaskNum() {
    return (int) failureTaskNum.get();
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

  public boolean isSnapshotAvailable(int snapshotId) {
    Path snapshotPath = new Path(snapshotDir, Integer.toString(snapshotId));
    try {
      return fs.exists(new Path(snapshotPath, "_SUCCESS"));
    } catch (Exception e) {
      LOG.warn("Failed to check snapshot availability, path "
          + snapshotPath + ", cause " + e);
      return false;
    }
  }

  public String getSnapshotDir(int snapshotId) {
    return new Path(snapshotDir, Integer.toString(snapshotId)).toString();
  }

  public List<TaskWrapper> getFailureTasksFromFs(PrimusConf primusConf, int limit) {
    try {
      String storeDir = getStoreDir(context.getApplicationNameForStorage(), name, primusConf);
      Path taskStatusesPath = new Path(storeDir, FAILURE_TASK_STATUSES_FILENAME);
      LOG.info("Start getFinishedTasksFromFs limit: {}", limit);
      return getFinishedTasksFromFs(fs, storeDir, taskStatusesPath, limit);
    } catch (IOException e) {
      LOG.warn("Failed to get failure tasks", e);
    }
    return new LinkedList<>();
  }

  private String getStoreDir(String appId, String name, PrimusConf primusConf) throws IOException {
    if (!primusConf.getInputManager().hasWorkPreserve()) {
      throw new IOException("Not set store directory for " + FileTaskStore.class.getSimpleName());
    }

    WorkPreserve workPreserve = primusConf.getInputManager().getWorkPreserve();
    HdfsConfig hdfsConfig = workPreserve.getHdfsConfig();
    return !workPreserve.hasHdfsConfig() || hdfsConfig.getStagingDir().isEmpty()
        ? primusConf.getStagingDir() + "/" + appId + "/state/" + name
        : hdfsConfig.getStagingDir() + "/" + name;
  }

  private List<TaskWrapper> getFinishedTasksFromFs(FileSystem fs, String storeDir,
      Path taskStatusesPath, int limit) throws IOException {
    List<TaskWrapper> taskWrappers = new LinkedList<>();
    if (!fs.exists(taskStatusesPath)) {
      return taskWrappers;
    }
    boolean limited = limit >= 0 ? true : false;

    List<TaskStatus> taskStatuses = new LinkedList<>();
    try (FSDataInputStream fsDataInputStream = FSDataInputStreamUtil
        .createFSDataInputStreamWithRetry(fs, taskStatusesPath)) {
      while (fsDataInputStream.available() > 0) {
        if (limited && limit-- <= 0) {
          LOG.info("getFinishedTask limitation reached.");
          break;
        }
        TaskStatusPBImpl taskStatus = new TaskStatusPBImpl();
        taskStatus.readFields(fsDataInputStream);
        taskStatuses.add(taskStatus);
      }
    } catch (IOException e) {
      LOG.warn("Failed to get task statuses from " + taskStatusesPath, e);
    }

    try {
      Map<Long, Integer> taskIdFileIdMap = getTasksIndexFromFs(fs, storeDir);
      for (TaskStatus taskStatus : taskStatuses) {
        Task task = getTaskFromFs(fs, storeDir, taskIdFileIdMap, taskStatus.getTaskId());
        task.setCheckpoint(taskStatus.getCheckpoint());
        task.setNumAttempt(taskStatus.getNumAttempt());
        TaskWrapper taskWrapper = new TaskWrapper(name, task, taskStatus);
        taskWrappers.add(taskWrapper);
      }
    } catch (IOException e) {
      LOG.warn("Failed to get task from directory " + storeDir, e);
    }
    return taskWrappers;
  }

  private static Map<Long, Integer> getTasksIndexFromFs(FileSystem fs, String storeDir)
      throws IOException {
    Path tasksIndexPath = new Path(storeDir, TASKS_INDEX_FILENAME);
    Map<Long, Integer> taskIdFileIdMap = new TreeMap<>();
    Path tmpTasksIndexPath = new Path(tasksIndexPath + ".tmp");
    if (!fileLockExists(fs, tasksIndexPath) && fs.exists(tmpTasksIndexPath)) {
      fs.delete(tasksIndexPath, true);
      fs.rename(tmpTasksIndexPath, tasksIndexPath);
    } else if (fs.exists(tasksIndexPath)) {
      try (FSDataInputStream fsDataInputStream = fs.open(tasksIndexPath)) {
        int maxFileId = 0;
        while (fsDataInputStream.available() > 0) {
          long taskId = fsDataInputStream.readLong();
          int fileId = fsDataInputStream.readInt();
          taskIdFileIdMap.put(taskId, fileId);
          maxFileId = Math.max(maxFileId, fileId);
        }
      }
    }
    return taskIdFileIdMap;
  }

  private static Task getTaskFromFs(FileSystem fs, String storeDir,
      Map<Long, Integer> taskIdFileIdMap, long taskId) throws IOException {
    int fileId = getFileId(taskIdFileIdMap, taskId);
    long position = getTaskPositionFromIndex(fs, storeDir, fileId, taskId);
    try (FSDataInputStream tasksInputStream =
        fs.open(new Path(storeDir, TASKS_FILENAME + "." + fileId))) {
      tasksInputStream.seek(position);
      TaskPBImpl task = new TaskPBImpl();
      task.readFields(tasksInputStream);
      return task;
    }
  }

  private static int getFileId(Map<Long, Integer> taskIdFileIdMap, long taskId) {
    ArrayList<Long> keys = new ArrayList<>(taskIdFileIdMap.keySet());
    int pos = Collections.binarySearch(keys, taskId);
    if (pos >= 0) {
      return taskIdFileIdMap.get(keys.get(pos));
    } else {
      pos = -pos - 1;  // Real insert index
      if (pos >= keys.size()) {
        return taskIdFileIdMap.get(keys.get(pos - 1));
      } else {
        return taskIdFileIdMap.get(keys.get(pos)) - 1;
      }
    }
  }

  private static long getTaskPositionFromIndex(
      FileSystem fs, String storeDir, int fileId, long taskId) throws IOException {
    Path indexPath = new Path(storeDir, TASKS_FILENAME + "." + fileId + ".index");
    try (FSDataInputStream indexIn = fs.open(indexPath)) {
      long firstTaskId = indexIn.readLong();
      long taskIndexPosition = (taskId - firstTaskId) * 2 * Long.BYTES;
      indexIn.seek(taskIndexPosition);
      if (indexIn.readLong() != taskId) {
        throw new IOException(
            "Failed to get task position from " + indexPath + " because this file is corrupt");
      }
      long taskPosition = indexIn.readLong();
      return taskPosition;
    }
  }

  class TaskSaver extends Thread {

    private int fileId;

    TaskSaver() {
      super(TaskSaver.class.getName() + "[" + name + "]");
      setDaemon(true);
      fileId = 0;
    }

    @Override
    public void run() {
      while (!isStopped) {
        if (!newTasks.isEmpty()) {
          try {
            taskFileLock.writeLock().lock();
            TimerMetric latency =
                PrimusMetrics.getTimerContextWithOptionalPrefix("am.taskstore.saver.latency");
            Task task = newTasks.poll();
            updateIndex(task.getTaskId(), fileId);
            Path tmpTasksPath = new Path(tasksPath + "." + fileId + ".tmp");
            Path tmpTasksIndexPath = new Path(tasksPath + "." + fileId + ".index.tmp");
            createFileLock(tmpTasksPath);
            FSDataOutputStream tasksOut = fs.create(tmpTasksPath, true);
            FSDataOutputStream tasksIndexOut = fs.create(tmpTasksIndexPath, true);
            Task lastSavedTask = task;
            while (task != null) {
              if (task != TASK_DELIMITER) {
                tasksIndexOut.writeLong(task.getTaskId());
                tasksIndexOut.writeLong(tasksOut.getPos());
                task.write(tasksOut);
                lastSavedTask = task;
              } else {
                tasksOut.close();
                tasksIndexOut.close();
                break;
              }
              task = newTasks.poll();
            }
            deleteFileLock(tmpTasksPath);

            Path taskPath = new Path(tasksPath + "." + fileId);
            commitTaskMetaFile(tmpTasksPath, taskPath);

            Path taskIndexPath = new Path(tasksPath + "." + fileId + ".index");
            commitTaskMetaFile(tmpTasksIndexPath, taskIndexPath);

            currentSavedTaskId.set(lastSavedTask.getTaskId());
            LOG.info("Saved tasks to " + tasksPath + "." + fileId
                + ", currentSavedTaskId: " + currentSavedTaskId.get());
            fileId += 1;
            writeTaskMeta(new Path(tasksPath + "." + fileId), lastSavedTask);
            latency.stop();
          } catch (IOException e) {
            LOG.error("Save tasks caught exception", e);
            logTaskBuildFailed(ExceptionUtils.getFullStackTrace(e));
            // TODO: Fail job?
          } finally {
            taskFileLock.writeLock().unlock();
          }
        } else {
          threadSleep(THREAD_SLEEP_MS);
        }
      }
    }

    private void commitTaskMetaFile(Path tmpTaskMetaFilePath, Path targetTaskMetaFilePath)
        throws IOException {
      if (fs.exists(targetTaskMetaFilePath)) {
        LOG.warn("target TaskMetaFile existed, path:{}", targetTaskMetaFilePath);
        fs.delete(targetTaskMetaFilePath, false);
      }
      boolean success = fs.rename(tmpTaskMetaFilePath, targetTaskMetaFilePath);
      if (!success) {
        LOG.warn("failed to commitTaskMetaFile, path:{}, target:{}", tmpTaskMetaFilePath,
            targetTaskMetaFilePath);
      }
    }

    public void logTaskBuildFailed(String errmsg) {
      String eventType = "BUILD_TASK_FAILED";
      JsonObject buildTaskFailed = new JsonObject();
      buildTaskFailed.addProperty("attemptId", context.getAppAttemptId().getAttemptId());
      buildTaskFailed.addProperty("errmsg", errmsg);
      context.getTimelineLogger().logEvent(eventType, buildTaskFailed.toString());
    }

    private void updateIndex(long taskId, int fileId) throws IOException {
      // Update tasks index, that is to atomically save taskIdFileIdMap to file system
      if (!taskIdFileIdMap.containsKey(taskId)) {
        taskIdFileIdMap.put(taskId, fileId);
        createFileLock(tasksIndexPath);
        Path tmpTasksIndexPath = new Path(tasksIndexPath + ".tmp");
        FSDataOutputStream fsDataOutputStream = fs.create(tmpTasksIndexPath, true);
        for (Map.Entry<Long, Integer> entry : taskIdFileIdMap.entrySet()) {
          fsDataOutputStream.writeLong(entry.getKey());
          fsDataOutputStream.writeInt(entry.getValue());
        }
        fsDataOutputStream.close();
        deleteFileLock(tasksIndexPath);
        fs.delete(tasksIndexPath, true);
        fs.rename(tmpTasksIndexPath, tasksIndexPath);
      }
    }

    private void writeTaskMeta(Path path, Task lastTask) {
      OutputStream out = null;
      try {
        if (fs.exists(tasksMetaPath)) {
          out = fs.append(tasksMetaPath);
        } else {
          out = fs.create(tasksMetaPath);
        }
      } catch (IOException e) {
        LOG.warn("Failed to create or open " + tasksMetaPath, e);
        return;
      }

      BufferedWriter writer = null;
      try {
        writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
        String key = null;
        if (lastTask.getSplitTask() != null) {
          key = lastTask.getSplitTask().getKey();
        }
        if (key != null) {
          writer.write(path + "," + key + "\n");
        }
      } catch (Exception e) {
        LOG.warn("Failed to write tasks meta to " + tasksMetaPath, e);
      } finally {
        try {
          if (writer != null) {
            writer.close();
          } else if (out != null) {
            out.close();
          }
        } catch (IOException e) {
          LOG.warn("Failed to close " + tasksMetaPath, e);
        }
      }
    }

    public void recover() throws IOException {
      // Recover tasksIdFileIdMap
      taskIdFileIdMap = getTasksIndexFromFs(fs, storeDir);
      if (taskIdFileIdMap.isEmpty()) {
        LOG.info("Empty task state");
        return;
      }
      int maxFileId = Collections.max(taskIdFileIdMap.values());
      // Set next fileId
      if (fs.exists(new Path(tasksPath + "." + maxFileId))) {
        fileId = maxFileId + 1;
        Path indexPath = new Path(tasksPath + "." + maxFileId + ".index");
        Path tmpIndexPath = new Path(indexPath + ".tmp");
        if (fs.exists(tmpIndexPath) && !fs.exists(indexPath)) {
          fs.rename(tmpIndexPath, indexPath);
        }
      } else {
        fileId = maxFileId;
      }

      // Recover lastSavedTask, currentSavedTaskId, totalTaskNum
      Path lastSavedPath = new Path(tasksPath + "." + (fileId - 1));
      FileStatus indexFileStatus = fs.getFileStatus(new Path(lastSavedPath + ".index"));
      long seekPos = (indexFileStatus.getLen() - Long.BYTES * 2);
      try (FSDataInputStream tasksIndexIn = fs.open(new Path(lastSavedPath + ".index"));
          FSDataInputStream tasksIn = fs.open(lastSavedPath)) {
        tasksIndexIn.seek(seekPos);
        long taskId = tasksIndexIn.readLong();
        long taskPosition = tasksIndexIn.readLong();
        tasksIn.seek(taskPosition);
        lastSavedTask = new TaskPBImpl();
        lastSavedTask.readFields(tasksIn);
        if (lastSavedTask.getTaskId() != taskId) {
          throw new IOException(
              "Failed to recover state from files of " + storeDir + " because state is corrupt");
        }
        currentSavedTaskId.set(taskId);
        totalTaskNum.set(taskId);
      }
    }
  }

  class TaskLoader extends Thread {

    TaskLoader() {
      super(TaskLoader.class.getName() + "[" + name + "]");
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        while (taskIdFileIdMap.size() <= 0 || currentSavedTaskId.get() < nextTaskIdToLoad) {
          threadSleep(THREAD_SLEEP_MS);
          if (isStopped) {
            return;
          }
        }
        int fileId = getFileId(taskIdFileIdMap, nextTaskIdToLoad);
        long taskPosition = getTaskPositionFromIndex(fs, storeDir, fileId, nextTaskIdToLoad);
        nextTaskIdToLoad = loadTasks(new Path(tasksPath + "." + fileId), taskPosition);

        while (!isStopped) {
          while (currentSavedTaskId.get() < nextTaskIdToLoad) {
            threadSleep(THREAD_SLEEP_MS);
            if (isStopped) {
              return;
            }
          }
          fileId += 1;
          nextTaskIdToLoad = loadTasks(new Path(tasksPath + "." + fileId), 0);
        }
      } catch (IOException e) {
        LOG.warn("Load tasks caught exception", e);
        // TODO: Fail job?
      }
    }

    private long loadTasks(Path path, long startPosition) throws IOException {
      try {
        TimerMetric latency =
            PrimusMetrics.getTimerContextWithOptionalPrefix("am.taskstore.loader.latency");
        FSDataInputStream tasksInputStream = fs.open(path);
        tasksInputStream.seek(startPosition);
        TaskPBImpl task = null;
        while (tasksInputStream.available() > 0 && !isStopped) {
          if (newPendingTasks.size() < MIN_PENDING_TASK_NUM) {
            task = new TaskPBImpl();
            task.readFields(tasksInputStream);
            newPendingTasks.add(task);
          } else {
            threadSleep(THREAD_SLEEP_MS);
          }
        }
        tasksInputStream.close();
        latency.stop();
        long ret = task.getTaskId() + 1;
        LOG.info("Load tasks from " + path + ", nextTaskIdToLoad " + ret);
        return ret;
      } catch (IOException e) {
        throw new IOException("Failed to load tasks from " + path, e);
      }
    }
  }

  class TaskPreserver extends Thread {

    private ExecutorService executorService;

    public TaskPreserver() {
      super(TaskPreserver.class.getName() + "[" + name + "]");
      setDaemon(true);
      executorService = Executors.newFixedThreadPool(3,
          new ThreadFactoryBuilder()
              .setNameFormat("TaskPreserver-%d")
              .setDaemon(true)
              .build()
      );
    }

    /**
     * Warning! HTTP Suspend won't create snapshot dir, gRPC Suspend with snapshotId != 0 will
     * create snapshot dir
     */
    @Override
    public void run() {
      int lastSnapshotId = 0;
      while (!isStopped) {
        FileTaskManager taskManager = (FileTaskManager) context.getDataStreamManager()
            .getTaskManager(name);
        if (taskManager != null && taskManager.isSuspend()
            && taskManager.getSnapshotId() > lastSnapshotId && isNoRunningTask()) {
          LOG.info("Start to preserve and snapshot state, last snapshot id " + lastSnapshotId
              + ", current snapshot id " + taskManager.getSnapshotId());
          preserve();
          lastSnapshotId = taskManager.getSnapshotId();
          Path snapshotPath = new Path(new Path(snapshotDir, name),
              Integer.toString(lastSnapshotId));
          snapshot(snapshotPath, false);
          deleteStaleSnapshot(lastSnapshotId);

          // Add a metric to check if snapshot id is useful
          PrimusMetrics.emitCounter("snapshot_id.app_count{app="
              + context.getAppAttemptId().getApplicationId() + "}", 1);
        }
        preserve();
        threadSleep(dumpIntervalSeconds);
      }
      executorService.shutdownNow();
    }

    private void preserve() {
      try {
        // prevent others thread to modify states of running/success/failure tasks
        readWriteLock.writeLock().lock();
        TimerMetric preserveLatency =
            PrimusMetrics.getTimerContextWithOptionalPrefix("am.preserve.latency");
        TimerMetric serializedLatency = PrimusMetrics
            .getTimerContextWithOptionalPrefix("am.preserve.serialized_latency");
        // Serialized and make a copy in memory to avoid big critical section
        List<byte[]> runningTaskStatuses = cloneRunningTaskStatuses();
        List<byte[]> successTaskStatuses = cloneSuccessTaskStatuses();
        List<byte[]> failureTaskStatuses = cloneFailureTaskStatuses();
        serializedLatency.stop();
        String preserveInfo = "Preserving: successTaskNum " + successTaskNum + ", maxSuccessTaskId "
            + maxSuccessTaskId
            + ", failureTaskNum " + failureTaskNum + ", maxFailureTaskId " + maxFailureTaskId
            + ", runningTaskNum " + runningTaskStatuses.size();
        readWriteLock.writeLock().unlock();

        TimerMetric writeLatency =
            PrimusMetrics.getTimerContextWithOptionalPrefix("am.preserve.write_latency");
        CompletionService<Integer> completion = new ExecutorCompletionService<>(executorService);
        completion.submit(() -> preserveRunningTasks(runningTaskStatuses));
        completion.submit(() -> preserveFinishTasks(successTaskStatuses, successTaskStatusesPath));
        completion.submit(() -> preserveFinishTasks(failureTaskStatuses, failureTaskStatusesPath));
        int size = 0;
        try {
          for (int i = 0; i < 3; i++) {
            size += completion.take().get();
          }
        } catch (InterruptedException | ExecutionException e) {
          LOG.info("Fallback to single thread preserving, multi-threads caught exception", e);
          size = preserveRunningTasks(runningTaskStatuses);
          size += preserveFinishTasks(successTaskStatuses, successTaskStatusesPath);
          size += preserveFinishTasks(failureTaskStatuses, failureTaskStatusesPath);
        }
        LOG.debug(preserveInfo);
        writeLatency.stop();
        preserveLatency.stop();
        PrimusMetrics.emitStoreWithOptionalPrefix("am.preserve.size", size);
        PrimusMetrics.emitStoreWithOptionalPrefix("am.preserve.tasks_num",
            runningTaskStatuses.size() + successTaskStatuses.size() + failureTaskStatuses.size());
      } catch (IOException e) {
        LOG.warn("Failed to save state of running tasks", e);
      } finally {
        if (readWriteLock.writeLock().isHeldByCurrentThread()) {
          readWriteLock.writeLock().unlock();
        }
      }
    }

    public boolean snapshot(Path snapshotPath, boolean includeTasks) {
      LOG.info("Start snapshot, path " + snapshotPath + ", includeTasks " + includeTasks);
      boolean result = true;
      Configuration conf = new Configuration();
      try {
        TimerMetric latency =
            PrimusMetrics.getTimerContextWithOptionalPrefix(
                "am.snapshot.latency{snapshot=" + snapshotPath + "}");
        if (!fs.exists(snapshotPath)) {
          fs.mkdirs(snapshotPath);
        }
        // prevent others thread to modify states of running/success/failure tasks
        readWriteLock.writeLock().lock();
        CompletionService<Boolean> completion = new ExecutorCompletionService<>(executorService);
        int futureNum = 0;
        if (fs.exists(runningTaskStatusesPath)) {
          LOG.info("Copy running tasks snapshot, snapshotPath " + snapshotPath);
          completion.submit(() -> FileUtil.copy(fs, runningTaskStatusesPath, fs,
              new Path(snapshotPath, RUNNING_TASK_STATUSES_FILENAME), false, conf));
          futureNum += 1;
        }
        if (fs.exists(successTaskStatusesPath)) {
          LOG.info("Copy success tasks snapshot, snapshotPath " + snapshotPath);
          completion.submit(() -> FileUtil.copy(fs, successTaskStatusesPath, fs,
              new Path(snapshotPath, SUCCESS_TASK_STATUSES_FILENAME), false, conf)
          );
          futureNum += 1;
        }
        if (fs.exists(failureTaskStatusesPath)) {
          LOG.info("Copy failure tasks snapshot, snapshotPath " + snapshotPath);
          completion.submit(() -> FileUtil.copy(fs, failureTaskStatusesPath, fs,
              new Path(snapshotPath, FAILURE_TASK_STATUSES_FILENAME), false, conf));
          futureNum += 1;
        }
        for (int i = 0; i < futureNum; i++) {
          completion.take().get();
        }
        readWriteLock.writeLock().unlock();

        if (includeTasks) {
          // copy task meta, task indices and tasks to snapshot path
          try {
            taskFileLock.writeLock().lock();
            result = copyFilesParallel(storeDir + "/task*", snapshotPath, copyThreadCnt);
          } finally {
            taskFileLock.writeLock().unlock();
          }
        }
        if (result) {
          fs.create(new Path(snapshotPath, "_SUCCESS"));
          LOG.info("Finish snapshot, snapshotPath: " + snapshotPath);
        } else {
          LOG.info("Failed to snapshot to path: " + snapshotPath);
        }
        latency.stop();
      } catch (IOException | InterruptedException | ExecutionException e) {
        LOG.warn("Failed to snapshot state of running tasks", e);
        result = false;
      } finally {
        if (readWriteLock.writeLock().isHeldByCurrentThread()) {
          readWriteLock.writeLock().unlock();
        }
        return result;
      }
    }

    void deleteStaleSnapshot(int latestSnapshotId) {
      try {
        for (FileStatus fileStatus : fs.listStatus(snapshotDir)) {
          if (fileStatus.isDirectory()) {
            int snapshotId = Integer.valueOf(fileStatus.getPath().getName());
            if (latestSnapshotId - RETAIN_SNAPSHOT_NUMBER >= snapshotId) {
              LOG.info("Try to delete stale snapshot: " + fileStatus.getPath());
              fs.delete(fileStatus.getPath(), true);
              LOG.info("Deleted stale snapshot: " + fileStatus.getPath());
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed to clean up stale snapshots", e);
      }
    }

    private int preserveFinishTasks(List<byte[]> taskStatuses, Path path) throws IOException {
      int ret = 0;
      if (!taskStatuses.isEmpty()) {
        FSDataOutputStream fsDataOutputStream = null;
        try {
          if (fs.exists(path)) {
            fsDataOutputStream = fs.append(path);
          } else {
            fsDataOutputStream = fs.create(path);
          }
          long oldPos = fsDataOutputStream.getPos();
          for (byte[] status : taskStatuses) {
            fsDataOutputStream.writeInt(status.length);
            fsDataOutputStream.write(status);
          }
          ret = (int) (fsDataOutputStream.getPos() - oldPos);
        } finally {
          if (fsDataOutputStream != null) {
            fsDataOutputStream.close();
          }
        }
      }
      return ret;
    }

    private int preserveRunningTasks(List<byte[]> taskStatuses) throws IOException {
      int ret = 0;
      if (!taskStatuses.isEmpty()) {
        Path tmpPath = new Path(runningTaskStatusesPath + ".tmp");
        createFileLock(tmpPath);
        FSDataOutputStream fsDataOutputStream = fs.create(tmpPath, true);
        CompressionOutputStream cos = compressionCodec.createOutputStream(fsDataOutputStream);
        DataOutputStream dataOutputStream = new DataOutputStream(cos);
        // write meta
        dataOutputStream.writeFloat(context.getProgressManager().getProgress());
        dataOutputStream.writeLong(successTaskNum.get());
        dataOutputStream.writeLong(maxSuccessTaskId.get());
        dataOutputStream.writeLong(failureTaskNum.get());
        dataOutputStream.writeLong(maxFailureTaskId.get());
        dataOutputStream.writeInt(taskStatuses.size());
        // write task status
        for (byte[] status : taskStatuses) {
          dataOutputStream.writeInt(status.length);
          dataOutputStream.write(status);
        }
        ret = dataOutputStream.size();
        dataOutputStream.close();
        deleteFileLock(tmpPath);
        try {
          readWriteLock.writeLock().lock();
          fs.delete(runningTaskStatusesPath, true);
          fs.rename(tmpPath, runningTaskStatusesPath);
        } finally {
          readWriteLock.writeLock().unlock();
        }
      }
      return ret;
    }

    public void recover() throws IOException {
      int snapshotId = context.getPrimusConf().getInputManager().getWorkPreserve().getSnapshotId();
      if (context.getAppAttemptId().getAttemptId() == 1 && snapshotId > 0) {
        LOG.info("Start to recover snapshot, snapshot id " + snapshotId);
        recoverSnapshot(snapshotId);
      }

      // Recover successTaskNum, maxSuccessTaskId, failureTaskNum, maxFailureTaskId,
      // startTaskIdToLoad and running task status
      Path tmpPath = new Path(runningTaskStatusesPath + ".tmp");
      if (!fs.exists(runningTaskStatusesPath) && !fs.exists(tmpPath)) {
        LOG.info("Emtpy running state");
        return;
      } else if (!fs.exists(runningTaskStatusesPath)
          && (fileLockExists(fs, tmpPath) || !fs.exists(tmpPath))) {
        throw new IOException(
            "Failed to recover because "
                + runningTaskStatusesPath
                + " is not found, and "
                + tmpPath
                + " is incomplete (file lock exists) or is not found");
      } else if (!fileLockExists(fs, tmpPath) && fs.exists(tmpPath)) {
        fs.delete(runningTaskStatusesPath, true);
        fs.rename(tmpPath, runningTaskStatusesPath);
      }

      FSDataInputStream taskStatusInputStream =
          FSDataInputStreamUtil.createFSDataInputStreamWithRetry(fs, runningTaskStatusesPath);
      CompressionInputStream compressionInputStream =
          compressionCodec.createInputStream(taskStatusInputStream);
      try (DataInputStream dataInputStream = new DataInputStream(compressionInputStream)) {

        // read meta
        context.getProgressManager().setProgress(dataInputStream.readFloat());
        successTaskNum.set(dataInputStream.readLong());
        maxSuccessTaskId.set(dataInputStream.readLong());
        failureTaskNum.set(dataInputStream.readLong());
        maxFailureTaskId.set(dataInputStream.readLong());
        // read task status and recover tasks
        int taskNum = dataInputStream.readInt();
        long maxRunningTaskId = 0;
        int threadNum = Math.min(32, Math.max(3, taskNum / 500));
        LOG.info("{} tasks need recover", taskNum);
        TimerMetric recoverTaskLatency =
            PrimusMetrics.getTimerContextWithOptionalPrefix("am.taskstore.recover_task.latency");
        ExecutorService executorService =
            Executors.newFixedThreadPool(
                threadNum,
                new ThreadFactoryBuilder()
                    .setNameFormat("TaskPreserver-recover-%d")
                    .setDaemon(true)
                    .build());
        List<Future<TaskWrapper>> taskList = Lists.newArrayList();
        for (int i = 0; i < taskNum; i++) {
          TaskStatus taskStatus = new TaskStatusPBImpl();
          taskStatus.readFields(dataInputStream);
          maxRunningTaskId = Math.max(taskStatus.getTaskId(), maxRunningTaskId);
          Future<TaskWrapper> task =
              executorService.submit(
                  () -> {
                    TimerMetric latency =
                        PrimusMetrics.getTimerContextWithOptionalPrefix(
                            "am.taskstore.load_task_from_fs.latency");
                    try {
                      Task newTask =
                          getTaskFromFs(fs, storeDir, taskIdFileIdMap, taskStatus.getTaskId());
                      newTask.setCheckpoint(taskStatus.getCheckpoint());
                      newTask.setNumAttempt(taskStatus.getNumAttempt());
                      return new TaskWrapper(name, newTask, taskStatus);
                    } finally {
                      latency.stop();
                    }
                  });
          taskList.add(task);
        }
        try {
          for (Future<TaskWrapper> task : taskList) {
            oldPendingTasks.add(task.get());
          }
        } catch (InterruptedException e) {
          // ignore
        } catch (ExecutionException e) {
          if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
          } else {
            throw new IOException(e.getCause());
          }
        } finally {
          recoverTaskLatency.stop();
          executorService.shutdownNow();
        }

        LOG.info("All tasks recover finished");
        nextTaskIdToLoad =
            Math.max(maxRunningTaskId, Math.max(maxSuccessTaskId.get(), maxFailureTaskId.get()))
                + 1;
      }
    }

    private void recoverSnapshot(int snapshotId) throws IOException {
      Configuration conf = new Configuration();
      Path snapshotPath = new Path(snapshotDir, Integer.toString(snapshotId));
      LOG.info("Start to recover snapshot: " + snapshotPath);
      TimerMetric latency =
          PrimusMetrics.getTimerContextWithOptionalPrefix("am.taskstore.recover_snapshot.latency");
      CompletionService<Boolean> completion = new ExecutorCompletionService<>(executorService);

      int futureNum = 6;
      completion.submit(() -> fs.delete(runningTaskStatusesPath, true));
      completion.submit(() -> fs.delete(new Path(runningTaskStatusesPath + ".tmp"), true));
      completion.submit(() -> fs.delete(new Path(runningTaskStatusesPath + ".tmp.lock"), true));
      completion.submit(() -> fs.delete(successTaskStatusesPath, true));
      completion.submit(() -> fs.delete(failureTaskStatusesPath, true));
      completion.submit(() ->
          FileUtil.copy(fs, new Path(snapshotPath, RUNNING_TASK_STATUSES_FILENAME), fs,
              runningTaskStatusesPath, false, conf));

      Path successTaskSnapshot = new Path(snapshotPath, SUCCESS_TASK_STATUSES_FILENAME);
      if (fs.exists(successTaskSnapshot)) {
        completion.submit(() ->
            FileUtil.copy(fs, successTaskSnapshot, fs,
                successTaskStatusesPath, false, conf)
        );
        futureNum += 1;
      }
      Path failureTaskSnapshot = new Path(snapshotPath, FAILURE_TASK_STATUSES_FILENAME);
      if (fs.exists(failureTaskSnapshot)) {
        completion.submit(() ->
            FileUtil.copy(fs, failureTaskSnapshot, fs,
                failureTaskStatusesPath, false, conf));
        futureNum += 1;
      }

      try {
        for (int i = 0; i < futureNum; i++) {
          completion.take().get();
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Failed to recover snapshot, snapshot id " + snapshotId, e);
        throw new IOException("Failed to recover snapshot, snapshot id " + snapshotId, e);
      } finally {
        latency.stop();
      }
    }
  }

  @Override
  public boolean isNoRunningTask() {
    boolean result = true;
    for (Map<Long, TaskWrapper> tasks : executorTaskMap.values()) {
      if (!tasks.isEmpty()) {
        result = false;
        break;
      }
    }

    // double check
    if (result == true) {
      readWriteLock.writeLock().lock();
      for (Map<Long, TaskWrapper> tasks : executorTaskMap.values()) {
        if (!tasks.isEmpty()) {
          result = false;
          break;
        }
      }
      readWriteLock.writeLock().unlock();
    }
    return result;
  }

  private static CompressionCodec getCompressionCodec() {
    return ReflectionUtils.newInstance(SnappyCodec.class, new Configuration());
  }

  private static boolean fileLockExists(FileSystem fs, Path path) throws IOException {
    return fs.exists(new Path(path + ".lock"));
  }

  private void createFileLock(Path path) {
    int retryCount = 0;
    while (true) {
      try {
        fs.create(new Path(path + ".lock"), true).close();
        break;
      } catch (java.io.IOException ex) {
        retryCount++;
        if (retryCount % 100 == 0) {
          LOG.warn("Create file lock got exception path:" + path, ex);
        }
        threadSleep(10 * 1000);
      }
    }
  }

  private void deleteFileLock(Path path) throws IOException {
    fs.delete(new Path(path + ".lock"), true);
  }

  private void threadSleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  @Override
  public void stop() {
    isStopped = true;
    taskPreserver.interrupt();
    taskLoader.interrupt();
    taskSaver.interrupt();
    try {
      taskPreserver.join();
      LOG.info("task preserver stopped");
      taskLoader.join();
      LOG.info("task loader stopped");
      taskSaver.join();
      LOG.info("task saver stopped");
    } catch (InterruptedException e) {
      LOG.warn("Stopping caught interrupted exception", e);
    }
  }

  private List<byte[]> cloneRunningTaskStatuses() {
    List<byte[]> runningTaskStatuses = new LinkedList<>();
    for (Map<Long, TaskWrapper> taskStatusMap : executorTaskMap.values()) {
      runningTaskStatuses.addAll(
          taskStatusMap.values()
              .stream()
              .map(taskWrapper -> taskWrapper.getTaskStatus().toByteArray())
              .collect(Collectors.toList()));
    }
    runningTaskStatuses.addAll(
        oldPendingTasks.stream()
            .map(taskWrapper -> taskWrapper.getTaskStatus().toByteArray())
            .collect(Collectors.toList())
    );

    return runningTaskStatuses;
  }

  private List<byte[]> cloneSuccessTaskStatuses() {
    List<byte[]> results = new LinkedList<>();
    TaskWrapper taskWrapper;
    // here remove task by poll() to avoid OOM
    while ((taskWrapper = successTasks.poll()) != null) {
      results.add(taskWrapper.getTaskStatus().toByteArray());
    }
    return results;
  }

  private List<byte[]> cloneFailureTaskStatuses() {
    List<byte[]> results = new LinkedList<>();
    TaskWrapper taskWrapper;
    // here remove task by poll() to avoid OOM
    while ((taskWrapper = failureTasks.poll()) != null) {
      results.add(taskWrapper.getTaskStatus().toByteArray());
    }
    return results;
  }

  @Override
  public boolean makeSavepoint(String savepointDir) {
    LOG.info("making savepoint " + savepointDir);
    Path snapshotPath = new Path(savepointDir, name);
    boolean result = taskPreserver.snapshot(snapshotPath, true);
    LOG.info("make savepoint " + savepointDir + " successful? " + result);
    return result;
  }

  private boolean copyFilesParallel(String filesPrefix, Path destPath, int parallel) {
    TimerMetric latency =
        PrimusMetrics.getTimerContextWithOptionalPrefix(
            "am.taskstore.copy_all_file.latency{dest_path=" + destPath + "}");
    LOG.info("Copy files " + filesPrefix + " to dest " + destPath + " in " + parallel + " threads");
    boolean result = true;
    try {
      Configuration conf = new Configuration();
      ExecutorService pool = Executors.newFixedThreadPool(
          parallel,
          new ThreadFactoryBuilder()
              .setNameFormat("CopyFilesParallel-" + destPath + "-%d")
              .setDaemon(true)
              .build()
      );

      List<Future<Boolean>> futures = new LinkedList<>();
      for (FileStatus fileStatus : fs.globStatus(new Path(filesPrefix))) {
        futures.add(
            pool.submit(
                () -> {
                  TimerMetric oneFilelatency =
                      PrimusMetrics.getTimerContextWithOptionalPrefix(
                          "am.taskstore.copy_one_file.latency{dest_path=" + destPath + "}");
                  try {
                    return FileUtil.copy(
                        fs,
                        fileStatus.getPath(),
                        fs,
                        new Path(destPath, fileStatus.getPath().getName()),
                        false,
                        true,
                        conf);
                  } catch (IOException e) {
                    LOG.error("Failed to copy file", e);
                    return false;
                  } finally {
                    if (oneFilelatency != null) {
                      oneFilelatency.stop();
                    }
                  }
                }));
      }
      for (Future<Boolean> future : futures) {
        result = future.get();
        if (!result) {
          break;
        }
      }
      pool.shutdownNow();
    } catch (IOException | InterruptedException | ExecutionException e) {
      LOG.warn("Failed to copy " + filesPrefix + " to " + destPath, e);
      result = false;
    } finally {
      latency.stop();
    }
    return result;
  }
}
