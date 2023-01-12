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

import static com.bytedance.primus.am.ApplicationExitCode.KUBERNETES_ENV_MISSING;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_ID_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_DRIVER_POD_UNIQ_ID_ENV_KEY;

import com.bytedance.primus.am.AMCommandParser;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.utils.ConfigurationUtils;
import java.util.HashSet;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesApplicationMasterMain {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesApplicationMasterMain.class);

  public static void main(String[] args) throws Exception {
    CommandLine cmd = AMCommandParser.getCmd(args);
    String configPath = cmd.getOptionValue(AMCommandParser.CONFIGURATION_PATH);
    PrimusConf primusConf = ConfigurationUtils.load(configPath);

    for (String key : new HashSet<String>() {{
      add(PRIMUS_APP_ID_ENV_KEY);
      add(PRIMUS_DRIVER_POD_UNIQ_ID_ENV_KEY);
    }}) {
      if (!System.getenv().containsKey(key)) {
        LOG.error("Missing Environment Key: " + key);
        System.exit(KUBERNETES_ENV_MISSING.getValue());
        throw new RuntimeException("placeholder to suppress compiler error");
      }
    }

    run(primusConf,
        System.getenv().get(PRIMUS_APP_ID_ENV_KEY),
        System.getenv().get(PRIMUS_DRIVER_POD_UNIQ_ID_ENV_KEY)
    );
  }

  public static void run(PrimusConf primusConf, String appId, String driverPodUniqId) {
    try (KubernetesApplicationMaster am = new KubernetesApplicationMaster(appId, driverPodUniqId)) {
      LOG.info("Metrics init ...");
      PrimusMetrics.init(primusConf.getRuntimeConf(), appId);

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
}
