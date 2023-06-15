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

package com.bytedance.primus.am.datastream;

import static com.bytedance.primus.common.util.PrimusConstants.DEFAULT_DATA_STREAM;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.datastream.env.EnvTaskManager;
import com.bytedance.primus.am.datastream.file.FileTaskManager;
import com.bytedance.primus.am.datastream.kafka.KafkaTaskManager;
import com.bytedance.primus.am.exception.PrimusAMException;
import com.bytedance.primus.apiserver.proto.DataProto.DataSourceSpec;
import com.bytedance.primus.apiserver.records.DataSpec;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.service.AbstractService;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamManager extends AbstractService implements
    EventHandler<DataStreamManagerEvent> {

  public static final String ENV_DATA_STREAM = "env";

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamManager.class);

  private AMContext context;
  private Map<String, TaskManager> taskManagerMap;
  private DataSpec dataSpec;

  public DataStreamManager(AMContext context) {
    super(DataStreamManager.class.getName());
    this.context = context;
    taskManagerMap = new ConcurrentHashMap<>();
    taskManagerMap.put(ENV_DATA_STREAM, new EnvTaskManager());
    Thread monitorThread = new MonitorThread();
    monitorThread.start();
  }

  public FileTaskManager getDefaultFileTaskManager() {
    return (FileTaskManager) getTaskManager(DEFAULT_DATA_STREAM);
  }

  public TaskManager getTaskManager(String dataStream) {
    return getTaskManagerMap().get(dataStream);
  }

  public Map<String, TaskManager> getTaskManagerMap() {
    return taskManagerMap;
  }

  public void suspend(int snapshotId) {
    LOG.info("Suspend data stream, snapshot id is " + snapshotId);
    for (TaskManager taskManager : taskManagerMap.values()) {
      taskManager.suspend(snapshotId);
    }
  }

  public void resume() {
    LOG.info("Resume data stream");
    for (TaskManager taskManager : taskManagerMap.values()) {
      taskManager.resume();
    }
  }

  @Override
  public void handle(DataStreamManagerEvent event) {
    Thread t = null;
    switch (event.getType()) {
      case DATA_STREAM_CREATED:
        t = new Thread(() -> createTaskManagers(event.getDataSpec(), event.getVersion()));
        break;
      case DATA_STREAM_UPDATE:
        t = new Thread(() -> updateTaskManagers(event.getDataSpec(), event.getVersion()));
        break;
      case DATA_STREAM_SUCCEED:
        t = new Thread(() -> succeedTaskManagers());
        break;
    }
    if (t != null) {
      t.setDaemon(true);
      t.start();
    }
  }

  private void createTaskManagers(DataSpec dataSpec, long version) {
    LOG.info("Create taskManagers");
    createOrUpdateTaskManagers(dataSpec, version);
    this.dataSpec = dataSpec;
  }

  private void updateTaskManagers(DataSpec dataSpec, long version) {
    LOG.info("Update taskManagers");
    createOrUpdateTaskManagers(dataSpec, version);
    removeTaskManager(dataSpec.getDataStreamSpecs().keySet(), version);
    this.dataSpec = dataSpec;
  }

  private void succeedTaskManagers() {
    LOG.info("Succeed taskManagers");
    for (Map.Entry<String, TaskManager> entry : taskManagerMap.entrySet()) {
      entry.getValue().succeed();
    }
  }

  private void updateExistingTaskManager(
      String dataStreamName,
      DataStreamSpec dataStreamSpec,
      String savepointDir,
      long version,
      TaskManager existingTaskManager
  ) {
    LOG.info("Update existing TaskManager with DataStreamSpec[{}]: {}",
        dataStreamName, dataStreamSpec);

    // Version check
    if (existingTaskManager.getVersion() > version) {
      LOG.info("TaskManager[{}] Current version {} less than target version {}, "
              + "do not recreate TaskManger",
          dataStreamName, version, existingTaskManager.getVersion());
      return;
    }

    // Identical dataStream spec?
    if (existingTaskManager.getDataStreamSpec().equals(dataStreamSpec)) {
      // With the same data savepoint?
      if (this.dataSpec.getDataSavepoint().equals(dataSpec.getDataSavepoint())) {
        LOG.info("TaskManager[" + dataStreamName
            + "]'s dataStreamSpec is not changed, "
            + "and DataSavepoint is not changed, so not recreate taskManager");
        return;
      }
    }

    // Let's deprecate the existing TaskManager and create a new one!
    if (Strings.isNullOrEmpty(savepointDir)) {
      context
          .emitFailApplicationEvent(
              String.format(
                  "Invalid dataStreamSpec change and non-empty savepoint for taskManager[%s]",
                  dataStreamName),
              ApplicationExitCode.DATA_INCOMPATIBLE.getValue());
      return;
    }

    LOG.info("TaskManager[" + dataStreamName
        + "]'s dataStreamSpec is not changed, DataSavepoint is changed, "
        + "so stop then recreate taskManager");
    removeTaskManagerExec(dataStreamName);
    createTaskManager(dataStreamName, dataStreamSpec, savepointDir, version);
  }

  private void createNewTaskManager(
      String dataStreamName,
      DataStreamSpec dataStreamSpec,
      String savepointDir,
      long version
  ) {
    LOG.info("Normal dataStream, start create TaskManager for DataStream[{}]: {}",
        dataStreamName, dataStreamSpec.getProto().toString());
    createTaskManager(dataStreamName, dataStreamSpec, savepointDir, version);
  }

  private void createOrUpdateTaskManagers(DataSpec dataSpec, long version) {
    String savepointDir = dataSpec
        .getDataSavepoint()
        .getDataSavepointSpec()
        .getSavepointDir();

    for (Map.Entry<String, DataStreamSpec> entry : dataSpec.getDataStreamSpecs().entrySet()) {
      String dataStringName = entry.getKey();
      DataStreamSpec dataStreamSpec = entry.getValue();
      if (taskManagerMap.containsKey(dataStringName)) {
        updateExistingTaskManager(
            dataStringName, dataStreamSpec,
            savepointDir, version,
            taskManagerMap.get(dataStringName));
      } else {
        createNewTaskManager(
            dataStringName, dataStreamSpec,
            savepointDir, version);
      }
    }
  }

  private void createTaskManager(String name, DataStreamSpec dataStreamSpec, String savepointDir,
      long version) {
    TaskManager taskManager = null;
    try {
      LOG.info("Create taskManager[" + name + "] with savepointDir " + savepointDir);
      DataSourceSpec dataSourceSpec = dataStreamSpec.getDataSourceSpecs().get(0).getProto();
      switch (dataSourceSpec.getDataSourceCase()) {
        case FILESOURCESPEC:
          taskManager = new FileTaskManager(context, name, dataStreamSpec, savepointDir, version);
          break;
        case KAFKASOURCESPEC:
          taskManager = new KafkaTaskManager(context, name, dataStreamSpec, version);
          break;
        default:
          throw new PrimusAMException(
              "Unsupported data source: " + dataSourceSpec.getDataSourceCase());
      }
    } catch (Exception e) {
      LOG.error("Failed to create taskManager for dataStream:[" + name + "]", e);
      String diag = "Failed to create taskManager for dataStream: "
          + dataStreamSpec.getProto()
          + ", exception: "
          + e;
      context
          .emitFailApplicationEvent(
              diag,
              ApplicationExitCode.DATA_STREAM_FAILED.getValue());

    } finally {
      if (taskManager != null) {
        taskManagerMap.put(name, taskManager);
      }
    }
  }

  private void removeTaskManager(Set<String> dataStreams, long version) {
    Iterator<Entry<String, TaskManager>> it = taskManagerMap.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, TaskManager> pair = it.next();
      String dataStream = pair.getKey();
      if (!dataStreams.contains(dataStream)) {
        if (pair.getValue().getVersion() <= version) {
          removeTaskManagerExec(dataStream);
        } else {
          LOG.info("Current version {} less than target version {}, do not remove TaskManger",
              version, pair.getValue().getVersion());
        }
      }
    }
  }

  private synchronized void removeTaskManagerExec(String taskManagerName) {
    if (!taskManagerName.equals(ENV_DATA_STREAM)) {
      LOG.info("Stop and remove taskManager " + taskManagerName);
      TaskManager taskManager = taskManagerMap.get(taskManagerName);
      taskManager.stop();
      taskManagerMap.remove(taskManagerName);
    }
  }

  class MonitorThread extends Thread {

    public MonitorThread() {
      super(MonitorThread.class.getName());
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(20 * 1000);
          if (isFailure()) {
            context.emitFailApplicationEvent(
                "TaskManager exceeds task failure percent, fail app",
                ApplicationExitCode.EXCEED_TASK_FAIL_PERCENT.getValue());
            return;
          }
          if (isSuccess()) {
            LOG.info("All task managers are successful");
            if (context.getApplicationMeta().getPrimusConf().getInputManager()
                .getGracefulShutdown()) {
              LOG.info("TaskManager's gracefulShutdown is true so not succeed app");
              continue;
            }
            LOG.info("All task managers are successful and succeed app");
            context.emitApplicationSuccessEvent(
                "datastreamManager success app",
                ApplicationExitCode.TASK_SUCCEED.getValue()
            );
          }
        } catch (Exception e) {
          LOG.warn("Ignore exception", e);
        }
      }
    }
  }

  private boolean isFailure() {
    for (Map.Entry<String, TaskManager> entry : taskManagerMap.entrySet()) {
      if (entry.getValue().isFailure()) {
        LOG.error("TaskManager[" + entry.getKey() + "] exceeds task failure percent");
        return true;
      }
    }
    return false;
  }

  private boolean isSuccess() {
    boolean success = taskManagerMap.isEmpty() ? false : true;
    for (Map.Entry<String, TaskManager> entry : taskManagerMap.entrySet()) {
      if (entry.getValue().isFailure()) {
        LOG.error("TaskManager[" + entry.getKey() + "] exceeds task failure percent");
        return false;
      }
      success = success && entry.getValue().isSuccess();
    }
    // TODO: remove me if default task manager ENV_INPUT is removed
    if (taskManagerMap.size() == 1) {
      success = false;
    }
    return success;
  }

  public boolean takeSnapshot(String directory) {
    int numTasks = taskManagerMap.size();
    ExecutorService pool = Executors.newFixedThreadPool(
        numTasks,
        new ThreadFactoryBuilder()
            .setNameFormat("Savepoint-" + directory + "-%d")
            .setDaemon(true)
            .build());
    List<Future<Boolean>> statuses = new LinkedList<>();
    for (TaskManager taskManager : taskManagerMap.values()) {
      statuses.add(pool.submit(() -> taskManager.takeSnapshot(directory)));
    }
    for (Future<Boolean> status : statuses) {
      try {
        if (!status.get()) {
          return false;
        }
      } catch (ExecutionException | InterruptedException e) {
        LOG.warn("makeSavepoint() failed", e);
        return false;
      }
    }
    boolean result = true;
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      fs.create(new Path(directory, "_SUCCESS"));
    } catch (IOException e) {
      LOG.warn("makeSavepoint() failed", e);
      result = false;
    } finally {
      pool.shutdownNow();
      return result;
    }
  }

  public DataSpec getDataSpec() {
    return dataSpec;
  }

  public void setDataSpec(DataSpec dataSpec) {
    this.dataSpec = dataSpec;
  }
}
