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

package com.bytedance.primus.am.state;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.exception.PrimusAMException;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.service.AbstractService;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

public class TaskPreserver extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskPreserver.class);

  private static final int DEFAULT_BUFFER_SIZE = 65536;
  private static final String TASK_ID = "TASK_ID";

  private AMContext context;
  private String jobId;
  private int dumpIntervalSecs;
  private StateStore stateStore;
  private Thread snapshotThread;
  private volatile boolean running;

  public TaskPreserver(AMContext context) throws PrimusAMException {
    super(TaskPreserver.class.getName());
    this.context = context;
    this.jobId = context.getAppAttemptId().getApplicationId().toString();
    this.dumpIntervalSecs = context.getPrimusConf().getInputManager()
        .getWorkPreserve().getDumpIntervalSecs();
    this.stateStore = createStateStore();
    this.running = false;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    running = true;
    snapshotThread = new Thread(() -> {
      while (running) {
        try {
          snapshot();
          Thread.sleep(dumpIntervalSecs);
        } catch (InterruptedException e) {
          return;
        }
      }
    });
    snapshotThread.setDaemon(true);
    snapshotThread.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    running = false;
    snapshotThread.interrupt();
    try {
      snapshotThread.join();
    } catch (InterruptedException e) {
      LOG.warn("", e);
    }
    super.serviceStop();
  }

  private StateStore createStateStore() throws PrimusAMException {
    switch (context.getPrimusConf().getInputManager().getWorkPreserve().getWorkPreserveType()) {
      case HDFS:
        return new HdfsStateStore(context);
      default:
        throw new PrimusAMException("Unsupported work preserve type");
    }
  }

  private void snapshot() {
    PrimusMetrics.TimerMetric latency =
        PrimusMetrics.getTimerContextWithOptionalPrefix("am.task_preserver.snapshot.latency");
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
    TaskManager taskManager = context.getDataStreamManager().getDefaultFileTaskManager();
    if (taskManager != null) {
      try {
        out.writeFloat(context.getProgressManager().getProgress());
        writeTaskStatuses(out, taskManager.getTasksForTaskPreserverSnapshot());
        out.close();
        byte[] state = Snappy.compress(byteArrayOutputStream.toByteArray());
        stateStore.saveTaskSnapshotState(jobId, TASK_ID, state);
        PrimusMetrics.emitStoreWithOptionalPrefix("am.task_preserver.snapshot.size", state.length);
      } catch (IOException e) {
        LOG.warn("Save snapshot failed", e);
      }
    }
    latency.stop();
  }

  public Map<Long, TaskStatus> getTaskStatuses() throws PrimusAMException {
    try {
      byte[] state = stateStore.loadTaskState(jobId, TASK_ID);
      if (state != null) {
        DataInputStream in =
            new DataInputStream(new ByteArrayInputStream(Snappy.uncompress(state)));
        context.getProgressManager().setProgress(in.readFloat());
        return readTaskStatuses(in);
      } else {
        return new HashedMap();
      }
    } catch (IOException e) {
      throw new PrimusAMException("Load state failed");
    }
  }

  private void writeTaskStatuses(DataOutput out, List<TaskWrapper> taskWrappers)
      throws IOException {
    out.writeInt(taskWrappers.size());
    for (TaskWrapper taskWrapper : taskWrappers) {
      taskWrapper.getTaskStatus().write(out);
    }
  }

  private Map<Long, TaskStatus> readTaskStatuses(DataInputStream in) throws IOException {
    Map<Long, TaskStatus> taskStatusMap = new HashedMap();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      TaskStatus taskStatus = new TaskStatusPBImpl();
      taskStatus.readFields(in);
      taskStatusMap.put(taskStatus.getTaskId(), taskStatus);
    }
    return taskStatusMap;
  }
}
