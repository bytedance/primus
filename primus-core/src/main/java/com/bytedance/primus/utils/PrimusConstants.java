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

package com.bytedance.primus.utils;

// TODO: Cleanup
public class PrimusConstants {

  public static final String SYSTEM_USER_ENV_KEY = "USER";

  public static final String PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY = "PRIMUS_SUBMIT_TIMESTAMP";
  public static final String PRIMUS_VERSION_ENV_KEY = "PRIMUS_VERSION";
  public static final String PRIMUS_CONF_DIR_ENV_KEY = "PRIMUS_CONF_DIR";
  public static final String PRIMUS_SBIN_DIR_ENV_KEY = "PRIMUS_SBIN_DIR";
  public static final String STAGING_DIR_KEY = "PRIMUS_STAGING_DIR";
  public static final String PRIMUS_CONF = "primus.conf";
  public static final String PRIMUS_CONF_PATH = "__primus_conf__";
  public static final String PRIMUS_JAR = "primus-STABLE.jar";
  public static final String PRIMUS_JAR_PATH = "__primus_lib__";
  public static final String EXPIRED_EXIT_MSG = "executor expired";
  public static final String BLACKLISTED_EXIT_MSG = "executor blacklisted";
  public static final String KILLED_THROUGH_AM_DIAG = "ApplicationMaster terminated by someone through http/rpc request";
  public static final String PRIMUS_AM_RPC_HOST = "PRIMUS_AM_RPC_HOST";
  public static final String PRIMUS_AM_RPC_PORT = "PRIMUS_AM_RPC_PORT";
  public static final String PRIMUS_HOME_ENV_KEY = "PRIMUS_HOME";
  // TODO: Maybe deprecate this
  // Use to specify the directory for primus-STABLE.jar to override PRIMUS_HOME_ENV_KEY
  public static final String PRIMUS_CORE_TARGET_KEY = "PRIMUS_CORE_TARGET";
  public static final String HDFS_SCHEME = "hdfs";
  public static final String LOG4J_PROPERTIES = "log4j2.xml";
  public static final String PRIMUS_EXECUTOR_UNIQUE_ID = "PRIMUS_EXECUTOR_UNIQUE_ID";

  public static final int DEFAULT_MAX_TASK_NUM_PER_WORKER = 1;
  public static final int DEFAULT_MAX_TASK_ATTEMPTS = 2;

  public static final int DEFAULT_FAILED_PERCENT = 50;

  public static final int DEFAULT_MESSAGE_BUFFER_SIZE = 4 * 1024 * 1024;

  public static final int DEFAULT_TASK_NUM_THRESHOLD = 10 * 1000;

  public static final int DEFAULT_SETUP_PORT_RETRY_MAX_TIMES = 30;

  public static final int DEFAULT_SETUP_PORT_WITH_BATCH_OR_GANG_SCHEDULER_RETRY_MAX_TIMES = 3;

  public static final int DEFAULT_MAX_ALLOWED_IO_EXCEPTION = 100;

  public static final int SCAN_INTERVAL_HOUR = 6;

  public static final int UPDATE_TO_API_SERVER_RETRY_TIMES = 3;
  public static final int UPDATE_TO_API_SERVER_RETRY_INTERVAL_MS = 2000;

  public static final String DAY_FORMAT_DEFAULT = "yyyyMMdd";
  public static final String DAY_FORMAT_DASH = "yyyy-MM-dd";
  public static final String DAY_FORMAT_RANGE = "yyyyMMdd0000";
  public static final String HOUR_FORMAT = "HH";
}
