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

package com.bytedance.primus.webapp;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.common.service.AbstractService;
import com.bytedance.primus.webapp.bundles.StatusBundle;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.VersionFieldSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsStore extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsStore.class);
  private static final int SNAPSHOT_INTERVAL_MS = 30000;
  private static final int BUFFER_SIZE = 81920;

  private AMContext context;
  private Thread snapshotThread;
  private final FileSystem fs;
  private Path historyFilePath;
  private Path tmpSavePath;
  private boolean success;
  private volatile boolean running;

  private ReentrantReadWriteLock readWriteLock;
  private StatusBundle statusBundle = null;
  private ObjectMapper mapper = new ObjectMapper();

  public HdfsStore(AMContext context) {
    super(HdfsStore.class.getName());
    this.context = context;
    this.fs = context.getHadoopFileSystem();
    this.success = true;
    this.readWriteLock = new ReentrantReadWriteLock();
  }

  public static Kryo getKryo() {
    final Kryo kryo = new Kryo();
    kryo.setDefaultSerializer(VersionFieldSerializer.class);
    kryo.register(Path.class);
    kryo.register(YarnProtos.URLProto.class);

    kryo.getRegistration(Path.class).setInstantiator(() -> new Path("/"));
    kryo.getRegistration(YarnProtos.URLProto.class)
        .setInstantiator(() -> YarnProtos.URLProto.newBuilder().buildPartial());

    return kryo;
  }

  private void save(HistorySnapshot snapshot) throws IOException {
    Kryo kryo = getKryo();
    FSDataOutputStream out = fs.create(tmpSavePath, true);
    Output output = new Output(out, BUFFER_SIZE);
    kryo.writeClassAndObject(output, snapshot);
    output.flush();
    output.close();
    if (fs.exists(historyFilePath)) {
      fs.delete(historyFilePath, false);
    }
    fs.rename(tmpSavePath, historyFilePath);
  }

  public synchronized void snapshot(boolean finished, boolean success) {
    try {
      LOG.info("Start snapshot...");
      HistorySnapshot snapshot = new HistorySnapshot(
          finished,
          success,
          StatusServlet.makeStatusBundle()
      );
      LOG.info("Saving history...");
      save(snapshot);
      LOG.info("Saved history to " + historyFilePath);

      // Update the latest status bundle
      readWriteLock.writeLock().lock();
      this.statusBundle = snapshot.getStatus();
      readWriteLock.writeLock().unlock();
    } catch (Exception e) {
      LOG.error("Failed to save history", e);
    }
  }

  public byte[] getStatusBundleJsonBytes() throws IOException {
    if (statusBundle != null) {
      try {
        readWriteLock.readLock().lock();
        return mapper.writeValueAsBytes(statusBundle);
      } catch (IOException e) {
        LOG.warn("Failed to get cached status bundle json bytes", e);
      } finally {
        readWriteLock.readLock().unlock();
      }
    }
    return mapper.writeValueAsBytes(StatusServlet.makeStatusBundle());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    String snapshotDir = new Path(
        context.getPrimusConf().getHistoryHdfsBase(),
        context.getMonitorInfoProvider().getHistorySnapshotSubdirectoryName()
    ).toString();

    String snapshotFileName = context
        .getMonitorInfoProvider()
        .getHistorySnapshotFileName();

    historyFilePath = new Path(snapshotDir, snapshotFileName);
    tmpSavePath = new Path(snapshotDir, snapshotFileName + ".tmp");

    this.running = true;
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    this.snapshotThread = new Thread(() -> {
      while (running) {
        try {
          Thread.sleep(SNAPSHOT_INTERVAL_MS);
        } catch (InterruptedException e) {
          return;
        }
        snapshot(false, false);
      }
    });
    this.snapshotThread.setDaemon(true);
    this.snapshotThread.start();
    super.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.stop();
    running = false;
    this.snapshotThread.interrupt();
    try {
      this.snapshotThread.join();
    } catch (InterruptedException e) {
      LOG.error("", e);
    }
    snapshot(true, success);
  }
}
