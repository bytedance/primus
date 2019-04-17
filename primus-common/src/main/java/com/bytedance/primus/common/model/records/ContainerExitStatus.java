/*
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
 *
 * This file may have been modified by Bytedance Inc.
 */

package com.bytedance.primus.common.model.records;

/**
 * Container exit statuses indicating special exit circumstances.
 */


public class ContainerExitStatus {

  public static final int SUCCESS = 0;
  public static final int INVALID = -1000;

  /**
   * Default container exit status
   */
  public static final int DEFAULT_EXIT_STATUS = -1;

  /**
   * Containers killed by the framework, either due to being released by the application or being
   * 'lost' due to node failures etc.
   */
  public static final int ABORTED = -100;

  /**
   * Containers preempted by the framework.
   */
  public static final int PREEMPTED = -102;

  /**
   * Container terminated because of exceeding allocated virtual memory.
   */
  public static final int KILLED_EXCEEDED_VMEM = -103;

  /**
   * Container terminated because of exceeding allocated physical memory.
   */
  public static final int KILLED_EXCEEDED_PMEM = -104;

  /**
   * Container was terminated by stop request by the app master.
   */
  public static final int KILLED_BY_APPMASTER = -105;

  /**
   * Container was terminated after the application finished.
   */
  public static final int KILLED_AFTER_APP_COMPLETION = -107;

  /**
   * Container was killed by the app master after success, which differs from KILLED_BY_APPMASTER.
   */
  public static final int KILLED_BY_APPMASTER_AFTER_SUCCESS = -110;

  /**
   * AM Container was killed when restarting.
   */
  public static final int KILLED_BY_APPMASTER_WHEN_RESTARTING = -111;

  /**
   * ========== NM ==========
   */
  /**
   * container launch failed when nm shrink resource
   */
  public static final int LAUNCH_FAILED_FOR_RESOURCE_SHRINK = -18001;

  /**
   * Container was killed by the container eviction manager.
   */
  public static final int KILLED_BY_CONTAINER_EVICTION_MANAGER = -1818;

  /**
   * Container terminated because of exceeding valid time range.
   */
  public static final int KILLED_EXCEEDED_VALID_TIME_RANGE = -1821;

  /**
   * Container terminated because of exceeding allocated disk size.
   */
  public static final int KILLED_EXCEEDED_DISK_SIZE = -10001;

  public static final int KILLED_EXCEEDED_LOG_DIR_DISK_SIZE = -10003;

  public static final int KILLED_EXCEEDED_WORK_DIR_DISK_SIZE = -10004;

  public static final int KILLED_EXCEEDED_SHUFFLE_DISK_SIZE = -10005;

  public static final int KILLED_EXCEEDED_TOTAL_DISK_SIZE = -10006;

  /**
   * Container terminated because of exceeding load limit.
   */
  public static final int KILLED_EXCEEDED_LOAD = -10002;

  public static final int KILLED_EXCEEDED_THREAD_LIMIT = -10007;

  public static final int LOCALIZATION_FAILED = -11001;

  public static final int INCORRECT_LOCALIZATION_PATH = -11002;

  public static final int HDFS_RESOURCE_LOCALIZATION_FAILED = -11003;

  public static final int LOCAL_RESOURCE_LOCALIZATION_FAILED = -11004;

  public static final int LOCAL_DISK_LOCALIZATION_FAILED = -11005;

  public static final int SUBMIT_LOCALIZATION_TASK_FAILED = -11006;

  /**
   * For Docker Runtime, it means pulling docker image.
   */
  public static final int RUNTIME_PREPARE_FAILED = -12005;

  public static final int NUMA_ALLOCATED_FAILED = -12006;

  public static final int NUMA_STRICT_BIND_FAILED = -12007;

  public static final int CGROUP_WRITE_FAILED = -12008;

  public static final int CONTAINER_ALREADY_DEACTIVATED = -12009;

  public static final int CONTAINER_ASSIGN_TPU_FAILED = -12010;

  public static final int CGROUP_OOM_KILLED = -12011;

  public static final int GPU_ASSIGNED_FAILED = -12023;

  public static final int CPUSET_ASSIGNED_FAILED = -12024;

  public static final int DOCKER_IMAGE_SHA256_CHECK_FAILED = -12025;

  public static final int PULL_DOCKER_IMAGE_CONNECTION_REFUSED = -12026;

  public static final int DOCKER_DAEMON_BREAKDOWN = -12027;

  public static final int CONNECT_HTTP2P_AGENT_FAILED = -12028;

  public static final int HTTP2P_AGENT_STATUS_UNHEALTHY = -12029;

  public static final int RETRY_PULL_DOCKER_IMAGE_INTERRUPTED = -12030;

  public static final int IMAGE_NOT_ALLOWED = -12031;

  /**
   * Container launch failed errors.
   */
  public static final int CONTAINER_LAUNCH_FAILED = -12000;

  public static final int CONTAINER_KILLED_BEFORE_LAUNCH = -12001;

  public static final int UNABLE_GET_LOCAL_RESOURCE = -12002;

  public static final int LOG_DIR_CREATION_FAILED = -12003;

  public static final int LOCAL_DIR_CREATION_FAILED = -12004;

  public static final int CREATE_DIR_OR_COPYFILE_ON_DISK_FAILED = -12012;

  public static final int ARGUMENTS_CHECK_FAILED = -12013;

  // avoid conflict, start from a new beginning
  public static final int PRIVATE_CONTAINER_SCRIPT_PATH_CREATION_FAILED = -12100;

  public static final int PRIVATE_TOKEN_FILE_CREATION_FAILED = -12101;

  public static final int PRIVATE_CLASS_PATH_JAR_DIR_CREATION_FAILED = -12102;

  public static final int PRIVATE_PID_FILE_CREATION_FAILED = -12103;

  public static final int PRIVATE_TOKEN_WRITE_FAILED = -12104;

  public static final int WRITE_LAUNCH_ENV_FAILED = -12105;

  public static final int WRITE_CREDENTIAL_TOKEN_FAILED = -12106;

  public static final int PERSIST_CONTAINER_LOG_DIR_FAILED = -12107;

  public static final int PERSIST_CONTAINER_WORK_DIR_FAILED = -12108;

  public static final int PERSIST_CONTAINER_LAUNCHED_STATE_FAILED = -12109;

  public static final int GET_CONTAINER_IP_FROM_ENV_FAILED = -12110;

  public static final int USER_LOG_DIR_MOUNT_CONFLICT_RELATIVE_MOUNT = -12111;

  public static final int MOUNT_ZENYA_FAILED_FOR_ROOT_SERVICE_DISABLED = -12112;

  public static final int MOUNT_BYTENAS_FAILED_FOR_ROOT_SERVICE_DISABLED = -12113;

  public static final int MOUNT_BYTENAS_FAILED_FOR_VOLUME_DSTPATH_UNKNOWN = -12114;

  public static final int UNABLE_PARSE_MOUNT_LIST_ENV = -12115;

  public static final int MOUNT_DIR_NOT_IN_WHITELIST = -12116;

  public static final int INVALID_MOUNT_MODE = -12117;

  public static final int INVALID_MOUNT_DIR = -12118;

  public static final int SOURCE_MOUNT_DIR_CREATE_FAILED = -12119;

  public static final int RUN_COMMAND_APPEND_ANNOTATIONS_FAILED = -12120;

  public static final int SOURCE_MOUNT_DIR_NOT_EXIST = -12121;

  public static final int USER_LOG_DIR_MOUNT_CONFLICT_DOCKER_MOUNTS = -12122;

  public static final int USER_LOG_DIR_MOUNT_CONFLICT_YARN_DEFAULT_MOUNT = -12123;

  public static final int DOCKER_INVALID_VOLUME_REQUEST = -12124;

  /**
   * Container recovery launch failed errors.
   */
  public static final int RECOVERED_CONTAINER_LAUNCH_FAILED = -12020;

  public static final int UNABLE_GET_PID = -12021;

  public static final int UNABLE_GET_EXIT_CODE = -12022;

  // avoid conflict, start from a new beginning
  public static final int UNABLE_GET_PID_PATH = -12201;

  public static final int FAILED_TO_CHECK_CONTAINER_ALIVENESS_VIA_PING_SIGNAL = -12202;

  public static final int FAILED_TO_CHECK_CONTAINER_ALIVENESS_VIA_COMMAND = -12203;

  public static final int UNABLE_LOCATE_PID_FILE = -12204;

  /**
   * Container was terminated by the decommissioning node.
   */
  public static final int KILLED_BY_DECOMMISSIONING_NODE = -13001;

  public static final int KILLED_BY_HTTP_API = -13002;

  public static final int MIGRATED_BY_HTTP_API = -13003;

  /**
   * When threshold number of the nodemanager-local-directories or threshold number of the
   * nodemanager-log-directories become bad.
   */
  public static final int DISKS_FAILED = -15001;

  public static final int LOCAL_DISK_WARN_KILL = -15002;

  public static final int LOG_DISK_WARN_KILL = -15003;

  public static final int TOKEN_DISK_WARN_KILL = -15004;

  public static final int SHUFFLE_USAGE_EXCEED_KILL_APP_THRESHOLD = -15005;

  public static final int LOCAL_DISK_FULL = -15006;

  public static final int LOG_DISK_FULL = -15007;

  public static final int TOKEN_DISK_FULL = -15008;

  public static final int LOCAL_DISK_ERROR = -15009;

  public static final int LOG_DISK_ERROR = -15010;

  public static final int TOKEN_DISK_ERROR = -15011;

  /**
   * ========== RM ==========
   */
  public static final int KILLED_BY_RESOURCEMANAGER = -21000;

  public static final int RELEASED_BY_APPMASTER = -21001;

  public static final int RELEASED_BY_APPATTEMPT_COMPLETED = -21002;

  public static final int RELEASED_BY_CONTAINER_EXPIRED = -21003;

  public static final int RELEASED_BY_NODE_REMOVE = -21004;

  public static final int ABORTED_BY_GANG_SCHEDULER = -21005;

  public static final int ABORTED_BY_CONTAINER_UNKNOWN_ON_CONTAINER_LAUNCH = -21006;

  public static final int ABORTED_BY_APPLICATION_UNKNOWN_ON_CONTAINER_LAUNCH = -21007;

  public static final int KILLED_BY_AM_HEARTBEAT_TIMEOUT = -21008;

  public static final int KILLED_BY_CONTAINER_OUT_OF_SYNC = -21010;

  public static final int RELEASE_BY_APPATTEMPT_ALLOCATE_FAILED = -21011;

  public static final int RELEASE_BY_APPATTEMPT_KILLED = -21012;

  public static final int RELEASE_BY_APPATTEMPT_UNEXPECT_AM_REGISTRATION = -21013;

  public static final int RELEASE_BY_APPATTEMPT_LAUNCH_FAILED = -21014;

  public static final int RELEASE_BY_APPATTEMPT_RESTART = -21015;

  public static final int KILLED_BY_AM_UNREGISTERED = -21016;

  public static final int RELEASED_BY_NODE_UNHEALTHY = -21017;

  public static final int RELEASED_BY_NODE_DECOMMISSIONED = -21018;

  public static final int RELEASED_BY_NODE_LOST_DUE_TO_EXPIRE = -21019;

  public static final int RELEASED_BY_NODE_REBOOTED = -21020;

  public static final int RELEASED_BY_NM_STORED_KILLED_COMMAND = -21021;

  public static final int KILLED_BY_APPATTEMPT_REMOVED = -21022;

  public static final int PREEMPTED_BY_UNIFIED_SCHEDULER = -21023;

  public static final int ABORTED_BY_REMOTE_GODEL_SCHEDULER = -22001;

  public static final int ABORTED_BY_ABSENT_GODEL_NMNODE = -22002;

  public static final int ABORTED_BY_ABNORMAL_GODEL_NMNODE = -22003;

  /**
   * Container was terminated in recovering.
   */
  public static final int RECOVER_ABORTED_BY_APPLICATION_UNKNOWN = -25001;

  public static final int RECOVER_ABORTED_BY_UNMANAGED_AM = -25002;

  public static final int RECOVER_ABORTED_BY_SCHEDULERAPP_UNKNOWN = -25003;

  public static final int RECOVER_ABORTED_BY_ALREADY_STOPPED_ATTEMPT = -25004;

  public static boolean isRMAbnormalExitStatus(int exitStatus) {
    return exitStatus == ABORTED
        || exitStatus == PREEMPTED
        || exitStatus == RELEASED_BY_APPMASTER
        || exitStatus == RELEASED_BY_APPATTEMPT_COMPLETED
        || exitStatus == RELEASED_BY_CONTAINER_EXPIRED
        || exitStatus == RELEASED_BY_NODE_REMOVE
        || exitStatus == RELEASED_BY_NODE_LOST_DUE_TO_EXPIRE
        || exitStatus == RELEASED_BY_NODE_DECOMMISSIONED
        || exitStatus == RELEASED_BY_NODE_UNHEALTHY
        || exitStatus == RELEASED_BY_NODE_REBOOTED
        || exitStatus == ABORTED_BY_GANG_SCHEDULER
        || exitStatus == ABORTED_BY_CONTAINER_UNKNOWN_ON_CONTAINER_LAUNCH
        || exitStatus == ABORTED_BY_APPLICATION_UNKNOWN_ON_CONTAINER_LAUNCH;
  }

  /**
   * Container was terminated by the descheduler when node load ratio exceeded constraint's node
   * load ratio or node had failed disk which influenced container running
   */
  public static final int KILLED_BY_DESCHEDULER = -16001;

  public static final int KILLED_BY_DESCHEDULER_WHEN_EXCEEDED_LOAD_RATIO = -16002;

  public static final int KILLED_BY_DESCHEDULER_WHEN_DISK_FAILED = -16003;

  /**
   * ========== RUNTIME ==========
   */
  public static final int UNABLE_TO_EXECUTE_CONTAINER_SCRIPT = 7;
}
