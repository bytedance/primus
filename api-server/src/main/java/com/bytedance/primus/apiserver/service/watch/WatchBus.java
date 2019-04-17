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

package com.bytedance.primus.apiserver.service.watch;

import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchEvent;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchBus {

  class WatchTask {

    String watchKey;
    WatchEvent watchEvent;

    public WatchTask(String watchKey, WatchEvent watchEvent) {
      this.watchKey = watchKey;
      this.watchEvent = watchEvent;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(WatchBus.class);

  private AtomicLong watchUidGenerator;
  // watch key -> watch uid -> watch interface
  private Map<String, Map<Long, WatchInterface>> watchMap;
  private BlockingQueue<WatchTask> watchEventQueue;
  private volatile boolean isStopped;
  private DispatchThread dispatchThread;

  public WatchBus() {
    watchUidGenerator = new AtomicLong(0);
    watchMap = new ConcurrentHashMap<>();
    watchEventQueue = new LinkedBlockingDeque<>();
    isStopped = false;
    dispatchThread = new DispatchThread();
    dispatchThread.start();
  }

  public long createWatch(String watchKey, WatchInterface watchInterface) {
    long watchUid = watchUidGenerator.addAndGet(1);
    synchronized (watchMap) {
      watchMap.putIfAbsent(watchKey, new ConcurrentHashMap<>());
      watchMap.get(watchKey).put(watchUid, watchInterface);
    }
    return watchUid;
  }

  public void cancelWatch(String watchKey, long watchUid) {
    Map<Long, WatchInterface> watches = watchMap.get(watchKey);
    if (watches != null) {
      watches.remove(watchUid);
      if (watches.isEmpty()) {
        synchronized (watchMap) {
          watchMap.remove(watchKey);
        }
      }
    }
  }

  public void post(String watchKey, WatchEvent watchEvent) throws InterruptedException {
    watchEventQueue.put(new WatchTask(watchKey, watchEvent));
  }

  private void dispatch(WatchTask watchTask) {
    int counter = 0;
    if (watchMap.containsKey(watchTask.watchKey)) {
      for (WatchInterface watchInterface : watchMap.get(watchTask.watchKey).values()) {
        watchInterface.onUpdate(watchTask.watchEvent);
        counter += 1;
      }
    }
    PrimusMetrics.emitCounterWithOptionalPrefix(
        "apiserver.watch_dispatch_count{key=" + watchTask.watchKey + "}",
        counter
    );
  }

  class DispatchThread extends Thread {

    public DispatchThread() {
      super(DispatchThread.class.getName());
      setDaemon(true);
    }

    @Override
    public void run() {
      while (!isStopped) {
        WatchTask watchTask = null;
        try {
          watchTask = watchEventQueue.take();
        } catch (InterruptedException e) {
          if (!isStopped) {
            LOG.warn(this.getName() + "interrupted", e);
          }
          return;
        }
        if (watchTask != null) {
          dispatch(watchTask);
        }
      }
    }
  }
}
