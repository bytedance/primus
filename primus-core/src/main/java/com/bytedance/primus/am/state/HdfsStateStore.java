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
import com.bytedance.primus.common.model.ApplicationConstants;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.proto.PrimusInput.WorkPreserve.HdfsConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsStateStore implements StateStore {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsStateStore.class);

  private String stagingDir;
  private Path stateStorePath;
  private Path stateStoreBackUpPath;
  private static final String TASK_STATE_STORE_NAME = "primus_status";
  private static final String TASK_STATE_STORE_BACK_UP_NAME = "primus_status.bak";
  private AtomicInteger index = new AtomicInteger(0);
  FileSystem dfs;

  public HdfsStateStore(AMContext context) {
    this.stagingDir = getStagingDir(context);
    String defaultFsPrefix = FileSystem.getDefaultUri(context.getHadoopConf()).toString();
    try {
      dfs = FileSystem.get(context.getHadoopConf());
      stateStorePath = new Path(stagingDir + "/" + TASK_STATE_STORE_NAME);
      stateStoreBackUpPath = new Path(stagingDir + "/" + TASK_STATE_STORE_BACK_UP_NAME);
    } catch (IOException e) {
      LOG.error("Failed to init hdfs state store", e);
      throw new RuntimeException("Failed to init hdfs state store");
    }
  }

  private String getStagingDir(AMContext context) {
    HdfsConfig hdfsConfig =
        context.getPrimusConf().getInputManager().getWorkPreserve().getHdfsConfig();
    if (!hdfsConfig.getStagingDir().isEmpty()) {
      return hdfsConfig.getStagingDir();
    } else {
      Map<String, String> envs = System.getenv();
      String cid = envs.get(ApplicationConstants.Environment.CONTAINER_ID.name());
      if (StringUtils.isEmpty(cid)) {
        cid = System.getProperty(ApplicationConstants.Environment.CONTAINER_ID.name());
      }
      ContainerId containerId = ContainerId.fromString(cid);
      ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
      return context.getPrimusConf().getStagingDir() + "/temp-dmlc-yarn-" +
          appAttemptID.getApplicationId().toString();
    }
  }

  @Override
  public byte[] loadTaskState(String jobId, String taskId) throws IOException {
    boolean isStateStorePathExisted = false;
    boolean isStateStoreBackupPathExisted = false;
    try {
      isStateStorePathExisted = dfs.exists(stateStorePath);
      isStateStoreBackupPathExisted = dfs.exists(stateStoreBackUpPath);
    } catch (IOException e) {
      // ignore
    }

    if (isStateStorePathExisted || isStateStoreBackupPathExisted) {
      FSDataInputStream in = null;
      try {
        if (isStateStorePathExisted) {
          in = dfs.open(stateStorePath);
        } else {
          in = dfs.open(stateStoreBackUpPath);
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] b = new byte[65536];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
          bos.write(b, 0, numBytes);
        }
        return bos.toByteArray();
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }

    LOG.warn("State path not existed: " + stateStorePath + ", " + stateStoreBackUpPath);
    return null;
  }

  @Override
  public void saveTaskJournalState(String jobId, String taskId, byte[] state) {
  }

  @Override
  public void saveTaskSnapshotState(String jobId, String taskId, byte[] state) {
    int tmpIndex = index.getAndIncrement();
    String tmpDir = stateStorePath + "." + tmpIndex;
    Path tmpPath = new Path(tmpDir);
    try {
      if (dfs.exists(tmpPath)) {
        dfs.delete(tmpPath, true);
      }
      FSDataOutputStream out = dfs.create(tmpPath);
      out.write(state);
      out.close();
      if (dfs.exists(stateStoreBackUpPath)) {
        dfs.delete(stateStoreBackUpPath, true);
      }
      if (dfs.exists(stateStorePath)) {
        dfs.rename(stateStorePath, stateStoreBackUpPath);
      }
      dfs.rename(tmpPath, stateStorePath);
    } catch (IOException e) {
      LOG.warn("Failed to save hdfs state store, dir is " + tmpDir, e);
    }
  }

  @Override
  public void init() {
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }
}
