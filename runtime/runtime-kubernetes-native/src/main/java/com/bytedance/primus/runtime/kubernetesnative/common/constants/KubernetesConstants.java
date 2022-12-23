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

package com.bytedance.primus.runtime.kubernetesnative.common.constants;

// TODO: Cleanup
public class KubernetesConstants {

  public static final String PRIMUS_DEFAULT_K8S_NAMESPACE = "default";

  public static final String PRIMUS_APP_ID_LABEL_NAME = "primus.k8s.io/app-id-selector";
  public static final String PRIMUS_APP_NAME_LABEL_NAME = "primus.k8s.io/app-name-selector";
  public static final String PRIMUS_APP_ID_ENV_KEY = "PRIMUS_APP_ID";
  public static final String PRIMUS_APP_NAME_ENV_KEY = "PRIMUS_APP_NAME";

  public static final String PRIMUS_DRIVER_POD_UNIQ_ID_ENV_KEY = "PRIMUS_AM_POD_UNIQ_ID";

  public static final String PRIMUS_ROLE_SELECTOR_LABEL_NAME = "primus.k8s.io/role-selector";
  public static final String PRIMUS_ROLE_DRIVER = "driver";
  public static final String PRIMUS_ROLE_EXECUTOR = "executor";

  public static final String PRIMUS_MOUNT_NAME = "primus-share";
  public static final String PRIMUS_MOUNT_PATH = "/opt/primus-share";

  /**
   * Environment
   */
  public static final String CONTAINER_SCRIPT_DIR_PATH = "container";
  public static final String CONTAINER_SCRIPT_PREPARE_ENV_FILENAME = "primus-env.sh";
  public static final String CONTAINER_SCRIPT_START_DRIVER_FILENAME = "start-driver.sh";
  public static final String CONTAINER_SCRIPT_START_EXECUTOR_FILENAME = "start-executor.sh";

  /**
   * Services
   */
  public static final int DRIVER_API_SERVER_PORT = 18080;
  public static final int DRIVER_EXECUTOR_TRACKER_SERVICE_PORT = 18090;

  /**
   * Pod
   **/
  public static final String PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME = "primus.k8s.io/executor-priority";
  public static final String SLEEP_SECONDS_BEFORE_POD_EXIT_ENV_KEY = "SLEEP_SECONDS_BEFORE_POD_EXIT";

  /**
   * Container
   **/
  public static final String FAKE_YARN_CONTAINER_ID_ENV_NAME = "CONTAINER_ID";
  public static final String PRIMUS_DEFAULT_IMAGE_PULL_POLICY = "Always";

  /**
   * Scheduler
   **/
  public static final String PRIMUS_AM_JAVA_MEMORY_XMX = "PRIMUS_AM_JAVA_MEMORY_XMX";
  public static final String PRIMUS_AM_JAVA_OPTIONS = "PRIMUS_AM_JAVA_OPTIONS";
  public static final String K8S_SCHEDULE_SCHEDULER_NAME_ANNOTATION_VALUE_DEFAULT = null; // Kubernetes Java API defaults to null.
  public static final String K8S_SCHEDULE_QUEUE_NAME_ANNOTATION_VALUE_DEFAULT = "default";
  public static final String K8S_SCHEDULE_SERVICE_ACCOUNT_NAME_DEFAULT = "default";

  /**
   * Security
   **/
  public static final String KUBERNETES_POD_META_LABEL_OWNER = "user";
}
