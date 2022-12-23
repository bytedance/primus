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

package com.bytedance.primus.runtime.yarncommunity.am;

import com.bytedance.primus.am.AMCommandParser;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMaster;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.ApplicationConstants.Environment;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.utils.ConfigurationUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnApplicationMasterMain {

  private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMasterMain.class);

  public static void main(String[] args) {
    try {
      // Obtain PrimusConf
      PrimusConf primusConf = ConfigurationUtils.load(
          AMCommandParser
              .getCmd(args)
              .getOptionValue(AMCommandParser.CONFIGURATION_PATH)
      );

      // Obtain ContainerId
      ContainerId containerId = getContainerId();

      // Init metrics
      PrimusMetrics.init(
          primusConf.getRuntimeConf(),
          containerId.getApplicationAttemptId().getApplicationId().toString()
      );

      // Start running
      run(primusConf, containerId);

    } catch (Throwable e) {
      LOG.error("Runtime error", e);
      System.exit(ApplicationExitCode.RUNTIME_ERROR.getValue());
    }
  }

  public static void run(PrimusConf primusConf, ContainerId containerId) throws Exception {
    try (ApplicationMaster am = new YarnApplicationMaster(primusConf, containerId)) {
      LOG.info("Initializing Application Master...");
      am.init();

      LOG.info("Starting Application Master...");
      am.start();

      int exitCode = am.waitForStop();
      LOG.info("Application master exit code: {}.", exitCode);
      System.exit(exitCode);
    }
  }

  private static ContainerId getContainerId() {
    String fromEnv = System.getenv().get(Environment.CONTAINER_ID.name());
    if (!StringUtils.isEmpty(fromEnv)) {
      return ContainerId.fromString(fromEnv);
    }
    String fromProperty = System.getProperty(Environment.CONTAINER_ID.name());
    if (!StringUtils.isEmpty(fromProperty)) {
      return ContainerId.fromString(fromProperty);
    }
    throw new RuntimeException("Failed to obtain ContainerId");
  }
}
