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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bytedance.blacklist.BlacklistTracker;
import com.bytedance.blacklist.BlacklistTrackerImpl;
import com.bytedance.primus.am.schedule.strategy.ContainerScheduleContext;
import com.bytedance.primus.am.schedule.strategy.ContainerStatusEnum;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.ApplicationId;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.impl.pb.ContainerPBImpl;
import java.util.Optional;
import org.junit.Test;

public class AddHostToBlacklistByContainerExitCodeStrategyTest {

  @Test
  public void testAddContainerIntoHost() {
    AddHostToBlacklistByContainerExitCodeStrategy strategy = new AddHostToBlacklistByContainerExitCodeStrategy();
    ContainerScheduleContext mock = mock(ContainerScheduleContext.class);
    BlacklistTracker blacklistTracker = new BlacklistTrackerImpl(1, 1, 1000, 1, 1);
    ApplicationAttemptId FAKE_YARN_APPLICATION_ID = ApplicationAttemptId
        .newInstance(
            ApplicationId.fromString("application_000_000"),
            0 /* attemptID */);
    ContainerId containerId = ContainerId.newContainerId(FAKE_YARN_APPLICATION_ID, 1);
    Container container = new ContainerPBImpl();
    container.setId(containerId);
    container.setIsGuaranteed(true);
    when(mock.getContainer()).thenReturn(container);
    when(mock.getExitCode()).thenReturn(-12005);
    when(mock.getHostName()).thenReturn("127.0.0.1");
    when(mock.getBlacklistTrackerOp()).thenReturn(Optional.of(blacklistTracker));
    strategy.updateStatus(ContainerStatusEnum.RELEASED, mock);

  }

}
