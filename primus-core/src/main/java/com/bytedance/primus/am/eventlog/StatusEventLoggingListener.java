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

package com.bytedance.primus.am.eventlog;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.service.AbstractService;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusEventLoggingListener extends AbstractService
    implements EventHandler<StatusEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(StatusEventLoggingListener.class);
  private static final int EVENT_QUEUE_CAPACITY = 10000;
  private static final StatusEvent POISON_PILL_EVENT = new StatusEvent(PrimusEventType.POISON_PILL);
  private static final int SNAPSHOT_INTERVAL_MS = 600000;

  private Queue<StatusEvent> eventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);

  private Thread dispatchThread;
  private Thread taskSnapshotThread;
  private EventSink eventSink;
  private volatile boolean running;
  private AMContext context;

  public StatusEventLoggingListener(AMContext context) throws Exception {
    super(StatusEventLoggingListener.class.getName());
    dispatchThread = new Thread(() -> dispatch());
    dispatchThread.setDaemon(true);
    dispatchThread.setName("StatusEventLoggingListenerDispatchThread");
    taskSnapshotThread = new Thread(() -> taskSnapshot());
    taskSnapshotThread.setDaemon(true);
    taskSnapshotThread.setName("StatusEventLoggingListenerTaskSnapshotThread");
    this.context = context;
    eventSink = new DummyEventSink();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    try {
      eventSink.init();
      this.running = true;
    } catch (Exception e) {
      LOG.warn("Failed to init bmq eventSink", e);
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      dispatchThread.start();
//      taskSnapshotThread.start();
    } catch (Exception e) {
      LOG.warn("Failed to start StatusEventLoggingListener", e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // TODO: might be blocking because full?
    try {
      eventQueue.add(POISON_PILL_EVENT);
      dispatchThread.join();
      running = false;
//      this.taskSnapshotThread.join();
      LOG.info("send task status event after finished~");
      context.getStatusEventWrapper().buildAllTaskStatusEvent().forEach(this::logEvent);
      eventSink.stop();
      super.serviceStop();
    } catch (Exception e) {
      LOG.warn("Failed to stop StatusEventLoggingListener", e);
    }
  }

  @Override
  public void handle(StatusEvent event) {
    if (eventQueue.offer(event)) {
      return;
    }
    LOG.warn(
        "Drop event {}. This likely means the listener is too slow and cannot keep up with the rate at which events are being created.",
        event);
  }

  private void taskSnapshot() {
    while (running) {
      try {
        Thread.sleep(SNAPSHOT_INTERVAL_MS);
      } catch (InterruptedException e) {
        return;
      }
      List<TaskStatusEvent> taskStatusEvents =
          context.getStatusEventWrapper().buildAllTaskStatusEvent();
      taskStatusEvents.forEach(context::logStatusEvent);
    }
  }

  private void dispatch() {
    StatusEvent event = eventQueue.poll();
    while (event != POISON_PILL_EVENT) {
      if (event != null) {
        logEvent(event);
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      event = eventQueue.poll();
    }
  }

  private void logEvent(StatusEvent event) {
    try {
      byte[] bytes = event.toByteArray();
      if (bytes != null) {
        eventSink.sink(bytes);
      }
    } catch (Exception e) {
      LOG.warn("Failed to log event " + event, e);
    }
  }

  public boolean canLogEvent() {
    return eventSink.canLogEvents();
  }
}
