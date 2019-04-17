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

package com.bytedance.primus.am.progress;

import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.service.AbstractService;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ProgressManager extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(ProgressManager.class);
  private static final int UPDATE_INTERVAL_MS = 30000;
  private float progress = 0.0f;
  private Thread updateThread;
  private volatile boolean running;

  public ProgressManager(String name) {
    super(name);
  }

  public synchronized float getProgress() {
    return progress;
  }

  public synchronized void setProgress(float progress) {
    setProgress(false, progress);
  }

  public synchronized void setProgress(boolean allowRewind, float progress) {
    if (allowRewind) {
      this.progress = progress;
      return;
    }
    if (progress > this.progress) {
      this.progress = progress;
    }
  }

  public abstract void update();

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() {
    running = true;
    updateThread = new Thread(new ProgressManagerUpdateThread(),
        ProgressManagerUpdateThread.class.getSimpleName());
    updateThread.setDaemon(true);
    updateThread.start();
    super.start();
  }

  @Override
  protected void serviceStop() {
    super.stop();
    running = false;
    updateThread.interrupt();
    try {
      updateThread.join();
    } catch (InterruptedException e) {
      LOG.warn("", e);
    }
  }

  class ProgressManagerUpdateThread implements Runnable {

    @Override
    public void run() {
      while (running) {
        try {
          Thread.sleep(UPDATE_INTERVAL_MS);
          update();
          PrimusMetrics.emitStoreWithOptionalPrefix("progress", (int) (getProgress() * 100));
        } catch (Exception e) {
          LOG.warn("Update progress catches error, retry", e);
        }
      }
    }
  }
}
