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
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.ApplicationConstants.Environment;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.utils.ConfigurationUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMasterMain {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterMain.class);

  public static void main(String[] args) throws Exception {
    CommandLine cmd = AMCommandParser.getCmd(args);
    String configPath = cmd.getOptionValue(AMCommandParser.CONFIGURATION_PATH);
    PrimusConf primusConf = ConfigurationUtils.load(configPath);

    ContainerId containerId = getContainerId();

    run(primusConf, containerId);
  }

  public static void run(PrimusConf primusConf, ContainerId containerId) {
    try (YarnApplicationMaster am = new YarnApplicationMaster(containerId)) {
      LOG.info("Metrics init ...");
      PrimusMetrics.init(
          primusConf.getRuntimeConf(),
          containerId.getApplicationAttemptId().getApplicationId().toString()
      );

      LOG.info("Application master init...");
      am.init(primusConf);

      LOG.info("Application master start...");
      am.start();

      int exitCode = am.waitForStop();
      LOG.info("Application master exit code: {}.", exitCode);
      System.exit(exitCode);

    } catch (Throwable e) {
      LOG.error("Runtime error", e);
      System.exit(ApplicationExitCode.RUNTIME_ERROR.getValue());
    }
  }

  private static ContainerId getContainerId() {
    String cid = System.getenv().get(Environment.CONTAINER_ID.name());
    if (!StringUtils.isEmpty(cid)) {
      return ContainerId.fromString(cid);
    }

    cid = System.getProperty(Environment.CONTAINER_ID.name());
    if (!StringUtils.isEmpty(cid)) {
      return ContainerId.fromString(cid);
    }

    throw new RuntimeException("Failed to construct ContainerId");
  }
}
