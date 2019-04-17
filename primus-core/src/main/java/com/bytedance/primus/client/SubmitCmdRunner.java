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

package com.bytedance.primus.client;

import com.bytedance.primus.utils.PrimusConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class SubmitCmdRunner implements ClientCmdRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SubmitCmdRunner.class);

  public String getConfDir() {
    Map<String, String> sysEnv = System.getenv();
    String confDir = sysEnv.get(PrimusConstants.PRIMUS_CONF_DIR_ENV_KEY);
    if (confDir == null) {
      LOG.error("Environment " + PrimusConstants.PRIMUS_CONF_DIR_ENV_KEY + " is not set");
      System.exit(1);
    }
    return confDir;
  }

  public String getSbinDir() {
    Map<String, String> sysEnv = System.getenv();
    String confDir = sysEnv.get(PrimusConstants.PRIMUS_SBIN_DIR_ENV_KEY);
    if (confDir == null) {
      LOG.error("Environment " + PrimusConstants.PRIMUS_SBIN_DIR_ENV_KEY + " is not set");
      System.exit(1);
    }
    return confDir;
  }
}
