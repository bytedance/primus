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

package com.bytedance.primus.am;

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.common.util.AbstractLivelinessMonitor;
import com.bytedance.primus.common.util.UTCClock;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorMonitor extends AbstractLivelinessMonitor<ExecutorId> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorMonitor.class);
  private final AMContext context;

  public ExecutorMonitor(AMContext context) {
    super(ExecutorMonitor.class.getName(), new UTCClock());
    this.context = context;
  }

  public void serviceInit(Configuration conf) {
    int heartbeatIntervalMs = context
        .getApplicationMeta()
        .getPrimusConf()
        .getScheduler()
        .getHeartbeatIntervalMs();
    int maxMissedHeartbeat = context
        .getApplicationMeta()
        .getPrimusConf()
        .getScheduler()
        .getMaxMissedHeartbeat();
    int expireInterval = heartbeatIntervalMs * maxMissedHeartbeat;

    setExpireInterval(expireInterval);
    setMonitorInterval(expireInterval);
  }

  @Override
  protected void expire(ExecutorId executorId) {
    context.emitExecutorExpiredEvent(executorId);
  }
}
