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

package com.bytedance.primus.am.controller;

import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.common.service.AbstractService;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuspendManager extends AbstractService {

  private static final Logger log = LoggerFactory.getLogger(SuspendManager.class);

  private final DataStreamManager dataStreamManager;

  public SuspendManager(DataStreamManager dataStreamManager) {
    super(SuspendManager.class.getSimpleName());
    this.dataStreamManager = dataStreamManager;
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
  }

  public void suspend(int snapshotId) {
    log.info("start suspend with ID:{}", snapshotId);
    dataStreamManager.suspend(snapshotId);
  }

  /**
   * Return the conclusive SuspendStatus of the entire SuspendManager.
   * <ul>
   *   <li>- NOT_STARTED -> all TaskManagers are NOT_STARTED </li>
   *   <li>- RUNNING -> at least one but every TaskManager is RUNNING </li>
   *   <li>- FINISHED_SUCCESS -> all TaskManagers are FINISHED_SUCCESS </li>
   * </ul>
   *
   * @return the SuspendStatus.
   */
  public SuspendStatusEnum suspendStatus() {
    return dataStreamManager.getTaskManagerMap().values().stream()
        .map(TaskManager::getSuspendStatus)
        .reduce(SuspendStatusEnum.FINISHED_SUCCESS,
            (acc, status) -> acc == SuspendStatusEnum.RUNNING // Stays in RUNNING
                ? SuspendStatusEnum.RUNNING
                : status == SuspendStatusEnum.RUNNING // Goes to RUNNING
                    ? SuspendStatusEnum.RUNNING
                    : status == SuspendStatusEnum.NOT_STARTED // Goes to NOT_STARTED
                        ? SuspendStatusEnum.NOT_STARTED
                        : acc);
  }

  public void resume() {
    log.info("start resuming");
    this.dataStreamManager.resume();
  }
}
