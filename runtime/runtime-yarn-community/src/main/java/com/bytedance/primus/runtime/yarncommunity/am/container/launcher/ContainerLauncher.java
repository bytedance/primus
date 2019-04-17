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

package com.bytedance.primus.runtime.yarncommunity.am.container.launcher;

import static com.bytedance.primus.utils.PrimusConstants.LOG4J_PROPERTIES;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_AM_RPC_HOST;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_AM_RPC_PORT;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF_PATH;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_EXECUTOR_UNIQUE_ID;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR_PATH;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY;
import static com.bytedance.primus.utils.PrimusConstants.STAGING_DIR_KEY;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceRequest;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceType;
import com.bytedance.primus.apiserver.records.ExecutorSpec;
import com.bytedance.primus.apiserver.records.impl.ExecutorSpecImpl;
import com.bytedance.primus.apiserver.records.impl.MetaImpl;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.executor.ContainerNumaBindingCheck;
import com.bytedance.primus.runtime.yarncommunity.am.YarnAMContext;
import com.bytedance.primus.runtime.yarncommunity.am.container.YarnRoleInfo;
import com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.basic.common.YarnNetworkManager;
import com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.basic.common.impl.YarnNetworkManagerFactory;
import com.bytedance.primus.runtime.yarncommunity.executor.ContainerMain;
import com.bytedance.primus.runtime.yarncommunity.utils.YarnConvertor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerLauncher implements EventHandler<ContainerLauncherEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerLauncher.class);

  private YarnAMContext context;
  private NMClient nmClient;
  private YarnNetworkManager yarnNetworkManager;

  private static final int MAX_RETRY_TIMES = 5;
  private static final int MAX_RETRY_INTERVAL_MS = 1000;

  public static final String PORT_URI = "yarn.io/port";

  public ContainerLauncher(YarnAMContext context) {
    this.context = context;
    nmClient = context.getNmClient();
    yarnNetworkManager = YarnNetworkManagerFactory.createNetworkManager(context);
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    switch (event.getType()) {
      case CONTAINER_ALLOCATED:
        new ContainerLauncherThread(event.getContainer()).start();
        break;
      case CONTAINER_UPDATED:
        new ContainerUpdaterThread(event.getContainer()).start();
        break;
    }
  }

  private List<String> buildBeforeExecutorStartCommands(RoleInfo roleInfo) {
    List<String> vargs = new ArrayList<>();
    int delayStartSeconds = roleInfo.getRoleSpec().getScheduleStrategy()
        .getExecutorDelayStartSeconds();
    if (delayStartSeconds > 0) {
      vargs.add("$JAVA_HOME/bin/java");
      Path childTmpDir = new Path(ApplicationConstants.Environment.PWD.$(),
          YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
      vargs.add("-Djava.io.tmpdir=" + childTmpDir);
      vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
          ApplicationConstants.LOG_DIR_EXPANSION_VAR);
      vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_SIZE + "=104857600");
      vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_BACKUPS + "=9");
      vargs.add("-Dhadoop.root.logfile=container-localizer-syslog");
      vargs.add(ContainerNumaBindingCheck.class.getName());
      vargs.add(String.format("--delay=%d", delayStartSeconds));
      vargs.add("&&");
    }
    return vargs;
  }

  public List<String> buildExecutorCommand(RoleInfo roleInfo, ExecutorId executorId) {
    List<String> vargs = new ArrayList<>();
    List<String> beforeExecutorStartCommands = buildBeforeExecutorStartCommands(roleInfo);
    if (beforeExecutorStartCommands.size() > 0) {
      vargs.addAll(beforeExecutorStartCommands);
    }
    vargs.add("$JAVA_HOME/bin/java");
    String parallelGCOption = String.format("-XX:ParallelGCThreads=%d", getCpuVCores(roleInfo));
    vargs.add(parallelGCOption);
    Path childTmpDir = new Path(ApplicationConstants.Environment.PWD.$(),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);
    vargs.add("-Dlog4j2.configurationFile=" + PRIMUS_CONF_PATH + "/" + LOG4J_PROPERTIES);
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_SIZE + "=104857600");
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_BACKUPS + "=9");
    vargs.add("-Dhadoop.root.logfile=container-localizer-syslog");
    vargs.add(roleInfo.getRoleSpec().getExecutorSpecTemplate().getJavaOpts());
    vargs.add(ContainerMain.class.getName());
    vargs.add("--am_host=" + context.getRpcAddress().getAddress().getHostAddress());
    vargs.add("--am_port=" + context.getRpcAddress().getPort());
    vargs.add("--role=" + executorId.getRoleName());
    vargs.add("--index=" + executorId.getIndex());
    vargs.add("--uniq_id=" + executorId.getUniqId());
    vargs.add("--apiserver_host=" + context.getApiServerHost());
    vargs.add("--apiserver_port=" + context.getApiServerPort());
    vargs.add("--conf=" + PRIMUS_CONF_PATH + "/" + PRIMUS_CONF);

    vargs.add("1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    Vector<String> vargsFinal = new Vector<>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;
  }

  private int getCpuVCores(RoleInfo roleInfo) {
    List<ResourceRequest> resourceRequests = roleInfo.getRoleSpec().getExecutorSpecTemplate()
        .getResourceRequests();
    int cores = resourceRequests.stream()
        .filter(t -> t.getResourceType() == ResourceType.VCORES)
        .findFirst()
        .get().getValue();
    return cores;
  }

  public void writeExecutorToApiServer(RoleInfo roleInfo, ExecutorId executorId) throws Exception {
    Executor executor = new Executor();
    executor.setMeta(new MetaImpl().setName(executorId.toUniqString()));
    ExecutorSpec executorSpec =
        new ExecutorSpecImpl(roleInfo.getRoleSpec().getExecutorSpecTemplate().getProto());
    executorSpec.setRoleIndex(executorId.getIndex());
    executor.setSpec(executorSpec);
    LOGGER.debug("Write executor to api server: " + context.getCoreApi().createExecutor(executor));
  }

  private void releaseContainer(Container container) {
    context.getAmRMClient().releaseAssignedContainer(container.getId());
  }

  class ContainerLauncherThread extends Thread {

    private Container container;

    public ContainerLauncherThread(Container container) {
      super(ContainerLauncherThread.class.getName() + "_" + container.getId());
      this.container = container;
      this.setDaemon(true);
    }

    @Override
    public void run() {
      // Register the new executor to ExecutorManager
      ExecutorId executorId = context
          .getSchedulerExecutorManager()
          .createExecutor(YarnConvertor.toPrimusContainer(container));
      if (executorId == null) {
        LOGGER.warn("Failed to create executor on " + container + ", so release container");
        releaseContainer(container);
        return;
      }

      // Update the new Role to API server
      LOGGER.info("Create executor" + executorId + " on container" + container);
      YarnRoleInfo roleInfo = (YarnRoleInfo)
          context.getRoleInfoManager()
              .getPriorityRoleInfoMap()
              .get(container.getPriority().getPriority());

      try {
        writeExecutorToApiServer(roleInfo, executorId);
      } catch (Exception e) {
        LOGGER.warn("Failed to write executor spec to api server, exception " + e
            + ", so release container " + container.getId() + ", executor " + executorId);
        releaseContainer(container);
        return;
      }

      // Launch the new container
      // TODO: Move to google retry
      int retryTimes = 1;
      Exception lastException;
      while (true) {
        try {
          PrimusMetrics.TimerMetric rpcLatency =
              PrimusMetrics.getTimerContextWithOptionalPrefix(
                  "am.container_launcher.start_container.latency");

          ContainerLaunchContext ctx = genContainerLaunchContext(container, executorId, roleInfo);
          nmClient.startContainer(container, ctx);

          rpcLatency.stop();
          PrimusMetrics.emitCounterWithOptionalPrefix(
              "am.container_launcher.start_container", 1);
          break;

        } catch (YarnException | IOException e) {
          LOGGER.error("Start container error for " + retryTimes + " times, exception ", e);
          lastException = e;
        }

        try {
          Thread.sleep(MAX_RETRY_INTERVAL_MS);
        } catch (InterruptedException e) {
          // ignore
        }

        if (++retryTimes > MAX_RETRY_TIMES) {
          LOGGER.warn("Start container " + container.getId() + " error for " + retryTimes
              + " times, last exception " + lastException + ", so release container");
          releaseContainer(container);
          return;
        }
      }
    }

    private ContainerLaunchContext genContainerLaunchContext(
        Container container,
        ExecutorId executorId,
        YarnRoleInfo roleInfo
    ) {
      ContainerLaunchContext containerLaunchContext =
          Records.newRecord(ContainerLaunchContext.class);

      containerLaunchContext.setCommands(buildExecutorCommand(roleInfo, executorId));

      LOGGER.debug("Launch worker: " + containerLaunchContext.getCommands());
      containerLaunchContext.setLocalResources(context.getLocalResources());

      Map<String, String> envs = new HashMap<>();
      StringBuilder classpath = new StringBuilder()
          .append(ApplicationConstants.Environment.CLASSPATH.$$())
          .append(":")
          .append(PRIMUS_JAR_PATH + "/*");
      for (String c : context.getHadoopConf().getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        classpath.append(":").append(c.trim());
      }
      envs.put("CLASSPATH", classpath.toString());
      envs.putAll(context.getPrimusConf().getEnvMap());
      envs.putAll(roleInfo.getRoleSpec().getExecutorSpecTemplate().getEnvs());
      envs.put(STAGING_DIR_KEY, context.getPrimusConf().getStagingDir());
      envs.put(PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY,
          context.getEnvs().getOrDefault(PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY, "0"));
      envs.put(PRIMUS_AM_RPC_HOST, context.getAmService().getHostName());
      envs.put(PRIMUS_AM_RPC_PORT, String.valueOf(context.getAmService().getPort()));
      envs.put(PRIMUS_EXECUTOR_UNIQUE_ID, executorId.toUniqString());

      Map<String, String> executorNetworkEnvMap = yarnNetworkManager
          .createNetworkEnvMap(executorId);
      envs.putAll(executorNetworkEnvMap);

      containerLaunchContext.setEnvironment(envs);

      return containerLaunchContext;
    }
  }

  class ContainerUpdaterThread extends Thread {

    private Container container;

    public ContainerUpdaterThread(Container container) {
      super(ContainerUpdaterThread.class.getName() + "_" + container.getId());
      this.container = container;
      this.setDaemon(true);
    }

    @Override
    public void run() {
      int retryTimes = 0;
      Exception lastException;
      while (true) {
        try {
          context.getNmClient().updateContainerResource(container);
          break;
        } catch (YarnException | IOException e) {
          LOGGER.error("Update container error for " + retryTimes + " times, exception ", e);
          lastException = e;
        }
        ++retryTimes;
        try {
          Thread.sleep(MAX_RETRY_INTERVAL_MS);
        } catch (InterruptedException e) {
          // ignore
        }
        if (retryTimes > MAX_RETRY_TIMES) {
          LOGGER.warn("Update container " + container + " error for " + retryTimes
              + " times, last exception " + lastException
              + ", release it and request new container");
          releaseContainer(container);
          return;
        }
      }
    }
  }
}
