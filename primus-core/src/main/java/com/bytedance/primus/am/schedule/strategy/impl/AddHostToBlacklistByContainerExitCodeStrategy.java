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

package com.bytedance.primus.am.schedule.strategy.impl;

import com.bytedance.primus.am.schedule.strategy.ContainerScheduleAction;
import com.bytedance.primus.am.schedule.strategy.ContainerScheduleContext;
import com.bytedance.primus.am.schedule.strategy.ContainerScheduleStrategy;
import com.bytedance.primus.am.schedule.strategy.ContainerStatusEnum;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddHostToBlacklistByContainerExitCodeStrategy implements ContainerScheduleStrategy {

  private static final Logger log = LoggerFactory
      .getLogger(AddHostToBlacklistByContainerExitCodeStrategy.class);

  private static final Set<Integer> NODE_BLACKLIST_BY_CONTAINER_EXIT_CODE = Sets.newHashSet(-12005);
  private static final long DEFAULT_ADD_HOST_BLACKLIST_TIMEOUT = TimeUnit.MILLISECONDS
      .convert(12, TimeUnit.HOURS);

  @Override
  public ContainerScheduleAction processNewContainer(ContainerScheduleContext scheduleContext) {
    return ContainerScheduleAction.ACCEPT_CONTAINER;
  }

  @Override
  public void postProcess(ContainerScheduleAction currentAction,
      ContainerScheduleContext scheduleContext) {
  }

  @Override
  public void updateStatus(ContainerStatusEnum statusEnum,
      ContainerScheduleContext scheduleContext) {
    if (NODE_BLACKLIST_BY_CONTAINER_EXIT_CODE.contains(scheduleContext.getExitCode())) {
      String containerId = scheduleContext.getContainer().getId().toString();
      String host = scheduleContext.getHostName();
      int exitCode = scheduleContext.getExitCode();
      if (scheduleContext.getBlacklistTrackerOp().isPresent()) {
        Map<String, Long> blacklistNode = ImmutableMap.of(host, DEFAULT_ADD_HOST_BLACKLIST_TIMEOUT);
        scheduleContext.getBlacklistTrackerOp().get().addNodeBlackList(blacklistNode);
        log.info("container:{} exit abnormally, code:{}, add host:{} into blacklist, timeout:{}.",
            containerId, exitCode, host, DEFAULT_ADD_HOST_BLACKLIST_TIMEOUT);
      }

    }
  }
}
