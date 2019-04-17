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
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventLoggingListener extends AbstractService implements EventHandler<PrimusEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(EventLoggingListener.class);
  private static final int EVENT_QUEUE_CAPACITY = 10000;
  private static final PrimusEvent POISON_PILL_EVENT = new PrimusEvent(PrimusEventType.POISON_PILL);

  private Queue<PrimusEvent> eventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);

  private Thread dispatchThread;
  private EventSink eventSink;

  public EventLoggingListener(AMContext context) {
    super(EventLoggingListener.class.getName());
    dispatchThread = new Thread(new Runnable() {
      @Override
      public void run() {
        dispatch();
      }
    });
    dispatchThread.setName("EventLoggingListenerDispatchThread");
    dispatchThread.setDaemon(true);
    eventSink = new HdfsEventSink(context);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    eventSink.init();
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    dispatchThread.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    // TODO: might be blocking because full?
    eventQueue.add(POISON_PILL_EVENT);
    dispatchThread.join();
    eventSink.stop();
  }

  @Override
  public void handle(PrimusEvent event) {
    if (eventQueue.offer(event)) {
      return;
    }
    LOG.warn("Drop event " + event + ". This likely means the listener is too slow "
        + "and cannot keep up with the rate at which events are being created."
    );
  }

  private void dispatch() {
    PrimusEvent event = eventQueue.poll();
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

  private void logEvent(PrimusEvent event) {
    try {
      String json = event.toJsonString();
      if (json != null) {
        eventSink.sink(json);
      }
    } catch (Exception e) {
      LOG.warn("Failed to log event " + event, e);
    }
  }
}
