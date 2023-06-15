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

package com.bytedance.primus.runtime.kubernetesnative.am;

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_ID_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_DRIVER_POD_UNIQ_ID_ENV_KEY;

import com.bytedance.primus.am.AMCommandParser;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMaster;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.util.PrimusConfigurationUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesApplicationMasterMain {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesApplicationMasterMain.class);

  public static void main(String[] args) {
    try {
      // Obtain PrimusConf
      PrimusConf primusConf = PrimusConfigurationUtils.load(
          AMCommandParser
              .getCmd(args)
              .getOptionValue(AMCommandParser.CONFIGURATION_PATH)
      );

      // Obtain ApplicationId
      String applicationId = getSystemEnv(PRIMUS_APP_ID_ENV_KEY);
      String driverPodUniqId = getSystemEnv(PRIMUS_DRIVER_POD_UNIQ_ID_ENV_KEY);

      // Init metrics
      PrimusMetrics.init(primusConf.getRuntimeConf(), applicationId);

      // Start running
      run(primusConf, applicationId, driverPodUniqId);

    } catch (Throwable e) {
      LOG.error("Runtime error", e);
      System.exit(ApplicationExitCode.RUNTIME_ERROR.getValue());
    }
  }

  public static void run(
      PrimusConf primusConf,
      String applicationId,
      String driverPodUniqId
  ) throws Exception {
    try (ApplicationMaster am = new KubernetesApplicationMaster(
        primusConf,
        applicationId,
        driverPodUniqId)
    ) {
      LOG.info("Initializing Application Master...");
      am.init();

      LOG.info("Starting Application Master...");
      am.start();

      int exitCode = am.waitForStop();
      LOG.info("Application master exit code: {}.", exitCode);
      System.exit(exitCode);
    }
  }

  private static String getSystemEnv(String key) {
    if (!System.getenv().containsKey(key)) {
      throw new RuntimeException("Missing environment variable: " + key);
    }
    String value = System.getenv().get(key);
    if (Strings.isNullOrEmpty(value)) {
      throw new RuntimeException("Invalid environment variable: " + key + " of value: " + value);
    }
    return value;
  }
}
