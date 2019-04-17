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

package com.bytedance.primus.executor.task.file;

import com.bytedance.primus.utils.timeline.TimelineLogger;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricToTimeLineEventBridge {

  private static final Logger LOG = LoggerFactory.getLogger(MetricToTimeLineEventBridge.class);
  private final int batchSize;
  private final String eventName;
  private Map<String, Long> collector = new HashMap<>();


  private int loopCounter = 0;
  private Gson gson = new Gson();
  private TimelineLogger timelineLogger;

  public MetricToTimeLineEventBridge(TimelineLogger timelineLogger, String eventName,
      int batchSize) {
    this.timelineLogger = timelineLogger;
    this.eventName = eventName;
    this.batchSize = batchSize;
    LOG.info("Create MetricToTimeLineEventBridge, eventName:{}, batchSize:{}", eventName,
        batchSize);
  }

  public void record(String metric, long durationNanoseconds) {
    long existedTimeMilliseconds = collector.getOrDefault(metric, 0L);
    long durationMilliseconds = TimeUnit.MILLISECONDS
        .convert(durationNanoseconds, TimeUnit.NANOSECONDS);
    collector.put(metric, existedTimeMilliseconds + durationMilliseconds);
  }

  public void flush() {
    if (loopCounter++ < batchSize) {
      return;
    }
    sendTimeLine();
    collector.clear();
    loopCounter = 0;
  }

  public void close() {
    if (collector.size() == 0) {
      return;
    }
    sendTimeLine();
    collector.clear();
  }

  private void sendTimeLine() {
    if (timelineLogger != null) {
      timelineLogger.logEvent(eventName, gson.toJson(collector));
    }
  }

}
