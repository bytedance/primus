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

import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.common.util.ConcurrentUtils;
import com.bytedance.primus.common.util.IntegerUtils;
import com.bytedance.primus.common.util.LockUtils;
import com.bytedance.primus.common.util.LockUtils.LockWrapper;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusInput.WorkPreserve;
import com.bytedance.primus.proto.PrimusInput.WorkPreserve.HdfsConfig;
import com.bytedance.primus.runtime.storage.filesystem.FileBuffer;
import com.bytedance.primus.runtime.storage.filesystem.FileLock;
import com.bytedance.primus.runtime.storage.filesystem.PrimusStateStorage;
import com.bytedance.primus.utils.RuntimeUtils;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaskStorage is responsible for persisting/loading tasks and checkpoints from the underlying
 * storage efficiently. Therefore, TaskStorage encapsulates all the path generation logics and
 * exercise file lock when necessary to ensure file consistency.
 */
class TaskStorage {

  // TODO: Make configurable
  private static final int MIN_TASK_WRAPPER_LOADER_THREADS = 1;
  private static final int MAX_TASK_WRAPPER_LOADER_THREADS = 32;
  private static final int TASK_INSERTION_RETRY_SEC = 3;

  /**
   * Constructed tasks are persisted in three layer of files 1. TaskFileIndex: Highest TaskId per
   * batch -> FileId 2. TaskDataIndex: TaskId -> position in the TaskDataFile of the corresponding
   * batch 3. TaskData: The file hosts the serialized tasks of the corresponding batch
   */
  private static final String TASKS_FILE_INDEX_FILENAME = "tasks.index";
  private static final String TASKS_DATA_FILENAME = "tasks";

  private static final String SUCCESS_TASK_STATUSES_FILENAME = "success_task_statuses";
  private static final String FAILURE_TASK_STATUSES_FILENAME = "failure_task_statuses";
  private static final String RUNNING_TASK_STATUSES_FILENAME = "running_task_statuses";

  private final Logger LOG;
  private final String name;
  private final FileSystemTaskStore owner;
  private final FileSystem fs;

  private final ReentrantReadWriteLock taskQueueLock = new ReentrantReadWriteLock();
  private final ExecutorService snapshotWorkerPool; // Worker pool for various snapshot needs.

  // Paths
  private final Path storeDir;

  private final Path taskFileIndexCachePath; // Stores the serialized IndexCache
  private final Path taskFilePath; // The base path for each task file

  private final Path runningTaskStatusesPath;
  private final Path successTaskStatusesPath;
  private final Path failureTaskStatusesPath;

  // States
  private final TaskFileIndexCache taskFileIndexCache;
  private volatile Task lastSavedTask;

  private TaskStorage(
      FileSystemTaskStore owner,
      Path storeDir
  ) throws IOException {
    this.LOG = LoggerFactory.getLogger(TaskStorage.class.getName() + "[" + owner.getName() + "]");
    this.name = owner.getName();
    this.owner = owner;
    this.fs = RuntimeUtils.loadHadoopFileSystem(
        owner.getContext()
            .getApplicationMeta()
            .getPrimusConf()
    );

    this.snapshotWorkerPool = Executors.newFixedThreadPool(
        IntegerUtils.ensurePositiveOrDefault(
            owner.getContext().getApplicationMeta().getPrimusConf().getSnapshotCopyThreadsCnt(),
            4
        ),
        new ThreadFactoryBuilder()
            .setNameFormat("TaskStorage-%d")
            .setDaemon(true)
            .build()
    );

    // Paths
    this.storeDir = storeDir;
    this.taskFileIndexCachePath = new Path(storeDir, TASKS_FILE_INDEX_FILENAME);
    this.taskFilePath = new Path(storeDir, TASKS_DATA_FILENAME);

    this.runningTaskStatusesPath = new Path(storeDir, RUNNING_TASK_STATUSES_FILENAME);
    this.successTaskStatusesPath = new Path(storeDir, SUCCESS_TASK_STATUSES_FILENAME);
    this.failureTaskStatusesPath = new Path(storeDir, FAILURE_TASK_STATUSES_FILENAME);

    // States
    taskFileIndexCache = newTaskFileIndexCache(LOG, this.fs, this.taskFileIndexCachePath);
    lastSavedTask = taskFileIndexCache.hasTasks()
        ? getTaskFromFs(taskFileIndexCache.getMaxTaskId())
        : null;
  }

  public static TaskStorage create(FileSystemTaskStore owner) throws IOException {
    return new TaskStorage(
        owner,
        getStoreDir(owner)
    );
  }

  public static TaskStorage create(
      FileSystemTaskStore owner,
      String savepointDir
  ) throws IOException {
    Path storeDir = getStoreDir(owner);
    if (!Strings.isNullOrEmpty(savepointDir)) {
      // TODO: Implement copying from savepoint for Primus API
      throw new UnsupportedOperationException("Custom SavePointDir is not supported yet.");
    }
    return new TaskStorage(owner, storeDir);
  }

  private static TaskFileIndexCache newTaskFileIndexCache(
      Logger log,
      FileSystem fs,
      Path taskFileIndexCachePath
  ) throws IOException {
    try (FileLock ignored = new FileLock(fs, taskFileIndexCachePath)) {
      try (FSDataInputStream fsDataInputStream = fs.open(taskFileIndexCachePath)) {
        log.info("Loading TaskFileIndex from {}", taskFileIndexCachePath);
        return TaskFileIndexCache.deserialize(fsDataInputStream);

      } catch (FileNotFoundException e) {
        log.info("No TaskFileIndex file found at {}, starting fresh", taskFileIndexCachePath);
        return new TaskFileIndexCache();

      } catch (IOException e) {
        throw new PrimusRuntimeException("Failed to recover taskIndexCache", e);
      }
    }
  }

  public Optional<Task> getLastSavedTask() {
    return Optional.ofNullable(lastSavedTask);
  }

  public boolean hasTasks() {
    return taskFileIndexCache.hasTasks();
  }

  public long getTaskNum() {
    return taskFileIndexCache.getTaskNum();
  }

  public long getMaxTaskId() {
    return taskFileIndexCache.getMaxTaskId();
  }

  // Path Utils ====================================================================================
  // ===============================================================================================

  private static Path getStoreDir(FileSystemTaskStore owner) throws IOException {
    PrimusConf primusConf = owner.getContext()
        .getApplicationMeta()
        .getPrimusConf();

    WorkPreserve workPreserve = primusConf
        .getInputManager()
        .getWorkPreserve();

    String applicationId = owner.getContext().getApplicationMeta().getApplicationId();
    String name = owner.getName();

    if (workPreserve.hasHdfsConfig()) {
      HdfsConfig hdfsConfig = workPreserve.getHdfsConfig();
      return hdfsConfig.getStagingDir().isEmpty()
          ? new Path(primusConf.getStagingDir() + "/" + applicationId + "/state/" + name)
          : new Path(hdfsConfig.getStagingDir() + "/" + name);
    }

    throw new IOException("Missing WorkPreserve: " + FileSystemTaskStore.class.getSimpleName());
  }

  private Path getTaskDataFilePath(int fileId) {
    return new Path(taskFilePath + "." + fileId);
  }

  private Path getTaskDataIndexFilePath(int fileId) {
    return new Path(taskFilePath + "." + fileId + ".index");
  }

  // TaskFile utils ================================================================================
  // ===============================================================================================

  // since only TaskFileIndexCache is a mutable file, locks are required with interacting with it.
  // In contrary, both TaskIndexFile and Task are immutable, and thus locks aren't required.

  public Optional<Pair<Integer, Task>> persistNewTasks(List<Task> tasks) throws IOException {
    try (LockWrapper ignoredM = LockUtils.lock(taskQueueLock.writeLock())) {
      if (tasks.isEmpty()) {
        LOG.warn("No task to persist");
        return Optional.empty();
      }

      // Start persisting, lock to mark whether tasks are successfully persisted.
      try (FileLock ignored = new FileLock(fs, taskFileIndexCachePath)) {
        // Obtain the FileId to persist current batch
        int fileId = taskFileIndexCache.getMaxFileId() + 1;

        // Create the task index file and task data file (order matters)
        try (
            FileBuffer taskIndexBuffer = new FileBuffer(fs, getTaskDataIndexFilePath(fileId));
            FileBuffer taskDataBuffer = new FileBuffer(fs, getTaskDataFilePath(fileId))
        ) {
          FSDataOutputStream taskIndexOut = taskIndexBuffer.getBufferedOutputStream();
          FSDataOutputStream taskDataOut = taskDataBuffer.getBufferedOutputStream();

          // Write task file
          for (Task task : tasks) {
            taskIndexOut.writeLong(task.getTaskId());
            taskIndexOut.writeLong(taskDataOut.getPos());
            task.write(taskDataOut);
          }
        }

        // Update and persist taskIndexCache
        Task maxIdTask = tasks.get(tasks.size() - 1);

        if (taskFileIndexCache.updateIndex(fileId, maxIdTask.getTaskId())) {
          try (FileBuffer taskFileIndexBuffer = new FileBuffer(fs, taskFileIndexCachePath)) {
            taskFileIndexCache.serialize(taskFileIndexBuffer.getBufferedOutputStream());
          } catch (Exception e) {
            // TODO(important): Retry and fail the entire application when necessary
            Log.warn("Failed to persist taskFileIndexCache[{}]: ", name, e);
          }
        }

        lastSavedTask = maxIdTask;
        return Optional.of(new Pair<>(fileId, maxIdTask));
      }
    }
  }

  public void loadNewTasksToVault(
      int fileId,
      long startPosition,
      TaskVault vault
  ) throws IOException {
    try (
        LockWrapper ignored = LockUtils.lock(taskQueueLock.readLock());
        DataInputStream iStream = createTaskInputStream(fileId, startPosition)
    ) {
      while (!owner.isStopped() && iStream.available() > 0) {
        // Parse one task from TaskFile
        TaskPBImpl task = new TaskPBImpl();
        task.readFields(iStream);
        // Keep inserting until success
        while (!owner.isStopped()) {
          if (!vault.addNewPendingTask(task)) {
            Sleeper.sleepWithoutInterruptedException(Duration.ofSeconds(TASK_INSERTION_RETRY_SEC));
          } else {
            break;
          }
        }
      }
    }
  }

  private DataInputStream createTaskInputStream(
      int fileId,
      long startPosition
  ) throws IOException {
    FSDataInputStream tasksInputStream = fs.open(getTaskDataFilePath(fileId));
    tasksInputStream.seek(startPosition);
    return tasksInputStream;
  }

  public Pair<Integer, Long> getFileIdAndPosition(long taskId) throws IOException {
    try (LockWrapper ignoredM = LockUtils.lock(taskQueueLock.readLock())) {
      // Get FileID
      int fileId = taskFileIndexCache.getFileId(taskId);
      // Get Position
      Path taskIndexPath = getTaskDataIndexFilePath(fileId);
      try (FSDataInputStream indexIn = fs.open(taskIndexPath)) {
        long firstTaskId = indexIn.readLong();
        long taskIndexPosition = (taskId - firstTaskId) * 2 * Long.BYTES;
        LOG.info(
            "TargetTaskId: {}, FirstTaskId: {}, Seeking to {}",
            taskId, firstTaskId, taskIndexPosition
        );
        indexIn.seek(taskIndexPosition);
        if (indexIn.readLong() != taskId) {
          throw new PrimusRuntimeException(
              "Failed to get position from corrupted index file: " + taskIndexPath);
        }
        return new Pair<>(fileId, indexIn.readLong());
      }
    }
  }

  private Task getTaskFromFs(long taskId) throws IOException {
    Pair<Integer, Long> index = getFileIdAndPosition(taskId);
    int fileId = index.getKey();
    long taskPosition = index.getValue();

    try (FSDataInputStream iStream = fs.open(getTaskDataFilePath(fileId))) {
      iStream.seek(taskPosition);
      TaskPBImpl task = new TaskPBImpl();
      task.readFields(iStream);
      return task;
    }
  }

  public List<TaskWrapper> loadTaskWrappers(List<TaskStatus> statuses) throws IOException {
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            IntegerUtils.ensureBounded(
                statuses.size() / 500,
                MIN_TASK_WRAPPER_LOADER_THREADS,
                MAX_TASK_WRAPPER_LOADER_THREADS
            ),
            new ThreadFactoryBuilder()
                .setNameFormat("TaskPreserver-recover-%d")
                .setDaemon(true)
                .build());
    try {
      // TODO: bulk load from underlying storage instead of one-by-one.
      LOG.info("Start recovering {} task(s)", statuses.size());
      return ConcurrentUtils.collect(
          executorService,
          statuses.stream()
              .map(status -> (Callable<TaskWrapper>) () -> {
                try (TimerMetric ignored = PrimusMetrics
                    .getTimerContextWithAppIdTag("am.taskstore.load_task_from_fs.latency")
                ) {
                  Task newTask = getTaskFromFs(status.getTaskId());
                  newTask.setCheckpoint(status.getCheckpoint());
                  newTask.setNumAttempt(status.getNumAttempt());
                  return new TaskWrapper(name, newTask, status);
                }
              })
              .collect(Collectors.toList()));

    } catch (ExecutionException | InterruptedException e) {
      throw new IOException("Failed to load TaskWrappers concurrently", e);

    } finally {
      executorService.shutdown();
    }
  }

  // Returns empty list on errors.
  public List<TaskWrapper> getSuccessTasksFromFs(int limit) {
    try (FileLock ignored = new FileLock(fs, successTaskStatusesPath)) {
      LOG.info("Start getFinishedTasksFromFs limit: {}", limit);
      return getFinishedTasksFromFs(successTaskStatusesPath, limit);
    } catch (IOException e) {
      LOG.warn("Failed to get success tasks", e);
    }
    return new LinkedList<>();
  }

  // Returns empty list on errors.
  public List<TaskWrapper> getFailureTasksFromFs(int limit) {
    try (FileLock ignored = new FileLock(fs, failureTaskStatusesPath)) {
      LOG.info("Start getFinishedTasksFromFs limit: {}", limit);
      return getFinishedTasksFromFs(failureTaskStatusesPath, limit);
    } catch (IOException e) {
      LOG.warn("Failed to get failure tasks", e);
    }
    return new LinkedList<>();
  }

  private List<TaskWrapper> getFinishedTasksFromFs(
      Path taskStatusesPath,
      int limit
  ) throws IOException {
    if (!fs.exists(taskStatusesPath)) {
      return new LinkedList<>();
    }

    // Collect task statuses
    boolean limited = limit >= 0;

    List<TaskStatus> taskStatuses = new LinkedList<>();
    try (FSDataInputStream inputStream = PrimusStateStorage.open(fs, taskStatusesPath)) {
      for (int cnt = 0; inputStream.available() > 0; ++cnt) {
        if (limited && cnt >= limit) {
          LOG.info("getFinishedTask limitation reached.");
          break;
        }
        TaskStatusPBImpl taskStatus = new TaskStatusPBImpl();
        taskStatus.readFields(inputStream);
        taskStatuses.add(taskStatus);
      }
    } catch (IOException e) {
      LOG.warn("Failed to get task statuses from " + taskStatusesPath, e);
    }

    // Build task wrappers
    return taskStatuses.stream()
        .flatMap(status -> {
          try {
            Task task = getTaskFromFs(status.getTaskId());
            task.setCheckpoint(status.getCheckpoint());
            task.setNumAttempt(status.getNumAttempt());
            return Stream.of(new TaskWrapper(name, task, status));

          } catch (IOException e) {
            LOG.warn(
                "Failed to get task wrapper for task: {} from directory: {}, error: {}",
                status.getTaskId(), storeDir, e);
            return Stream.empty();
          }
        })
        .collect(Collectors.toList());
  }

  // Checkpoint utils ==============================================================================
  // ===============================================================================================

  public Checkpoint loadCheckpoint() throws IOException {
    try (FileLock ignoredR = new FileLock(fs, runningTaskStatusesPath)) {
      return Checkpoint.deserialize(fs, runningTaskStatusesPath);
    }
  }

  // Returns bytes written onto the underlying storage.
  public long persistCheckpoint(Checkpoint checkpoint) throws IOException {
    try (
        FileLock ignoredR = new FileLock(fs, runningTaskStatusesPath);
        FileLock ignoredS = new FileLock(fs, successTaskStatusesPath);
        FileLock ignoredF = new FileLock(fs, failureTaskStatusesPath);
    ) {
      List<Callable<Long>> works = new LinkedList<Callable<Long>>() {{
        add(() -> checkpoint.preserveRunningTaskAndStatistics(fs, runningTaskStatusesPath));
        add(() -> checkpoint.preserveSuccessTasks(fs, successTaskStatusesPath));
        add(() -> checkpoint.preserveFailureTasks(fs, failureTaskStatusesPath));
      }};

      LOG.info("Start persisting task status in parallel");
      return ConcurrentUtils
          .reduce(snapshotWorkerPool, works, Long::sum)
          .orElse(0L);

    } catch (Exception e) {
      LOG.error("Failed to persist checkpoint: ", e);
      throw new IOException(e);
    }
  }

  // Snapshot utils ================================================================================
  // ===============================================================================================

  public void snapshot(Path snapshotPath) throws IOException {
    try (FileLock ignored = new FileLock(fs, snapshotPath)) {
      // Copy TaskQueue files
      copyTaskFiles(snapshotWorkerPool, storeDir + "/task*", snapshotPath);
      // Copy TaskStatuses
      snapshotTaskStatus(snapshotPath);
      fs.create(new Path(snapshotPath, "_SUCCESS"));
    }
  }

  // Copy snapshot files from srcDirPath to dstDirPath, returns if all succeed.
  private void copyTaskFiles(
      ExecutorService workerPool,
      String srcDirPath,
      Path dstDirPath
  ) throws IOException {
    LOG.info("Start copying snapshot files from {} to {}", srcDirPath, dstDirPath);
    try (
        LockWrapper ignoredF = LockUtils.lock(taskQueueLock.readLock());
        TimerMetric ignoredM = PrimusMetrics
            .getTimerContextWithAppIdTag(
                "am.taskstore.copy_all_task_files.latency",
                "dest_path", dstDirPath.toString()
            )
    ) {
      PrimusStateStorage.ensureDirectory(fs, dstDirPath);
      ConcurrentUtils.collect(
          workerPool,
          Arrays
              .stream(fs.globStatus(new Path(srcDirPath)))
              .map(file -> (Callable<Boolean>) () -> copyOneTaskFile(file.getPath(), dstDirPath))
              .collect(Collectors.toList())
      );
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new IOException(String.format("Failed to copy %s to %s", srcDirPath, dstDirPath), e);
    }
  }

  // Returns if srcPath has been copied to dstDirPath.
  private boolean copyOneTaskFile(Path srcPath, Path dstDirPath) throws IOException {
    try (TimerMetric ignored = PrimusMetrics
        .getTimerContextWithAppIdTag(
            "am.taskstore.copy_one_task_file.latency",
            "dest_path", dstDirPath.toString()
        )
    ) {
      return FileUtil.copy(
          fs, srcPath,
          fs, new Path(dstDirPath, srcPath.getName()),
          false /* deleteSource */,
          true /* overwrite */,
          fs.getConf()
      );
    } catch (IOException e) {
      LOG.warn("Failed to copy file: {}, err: {}", srcPath, e);
      throw new IOException(String.format("Failed to copy %s to %s", srcPath, dstDirPath), e);
    }
  }

  // Returns if succeeds.
  public void snapshotTaskStatus(Path snapshotPath) {
    try (
        FileLock ignoredR = new FileLock(fs, runningTaskStatusesPath);
        FileLock ignoredS = new FileLock(fs, successTaskStatusesPath);
        FileLock ignoredF = new FileLock(fs, failureTaskStatusesPath);
    ) {
      // Curate works
      List<Callable<Boolean>> works = new LinkedList<Callable<Boolean>>() {{
        if (fs.exists(runningTaskStatusesPath)) {
          LOG.info("Copy running tasks snapshot, snapshotPath " + snapshotPath);
          add(() -> FileUtil.copy(
              fs, runningTaskStatusesPath,
              fs, new Path(snapshotPath, RUNNING_TASK_STATUSES_FILENAME),
              false, fs.getConf()));
        }
        if (fs.exists(successTaskStatusesPath)) {
          LOG.info("Copy success tasks snapshot, snapshotPath " + snapshotPath);
          add(() -> FileUtil.copy(
              fs, successTaskStatusesPath,
              fs, new Path(snapshotPath, SUCCESS_TASK_STATUSES_FILENAME),
              false, fs.getConf())
          );
        }
        if (fs.exists(failureTaskStatusesPath)) {
          LOG.info("Copy failure tasks snapshot, snapshotPath " + snapshotPath);
          add(() -> FileUtil.copy(
              fs, failureTaskStatusesPath,
              fs, new Path(snapshotPath, FAILURE_TASK_STATUSES_FILENAME),
              false, fs.getConf()));
        }
      }};
      // Start snapshotting
      ConcurrentUtils.reduce(snapshotWorkerPool, works, Boolean::logicalAnd);

    } catch (ExecutionException | InterruptedException | IOException e) {
      LOG.warn("Failed to snapshot task status to path={}, err={}", snapshotPath, e);
    }
  }
}
