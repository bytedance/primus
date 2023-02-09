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

import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APP_SAVE_HISTORY;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APP_STOP_COMPONENT;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APP_STOP_NM;
import static com.bytedance.primus.utils.PrimusConstants.HDFS_SCHEME;
import static com.bytedance.primus.utils.PrimusConstants.LOG4J_PROPERTIES;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF_PATH;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR_PATH;
import static com.bytedance.primus.utils.PrimusConstants.STAGING_DIR_KEY;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationMaster;
import com.bytedance.primus.am.PrimusApplicationMeta;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.util.IntegerUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.yarncommunity.am.container.YarnRoleInfoFactory;
import com.bytedance.primus.runtime.yarncommunity.am.container.launcher.YarnContainerLauncher;
import com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.fair.FairContainerManager;
import com.bytedance.primus.runtime.yarncommunity.runtime.monitor.MonitorInfoProviderImpl;
import com.bytedance.primus.runtime.yarncommunity.utils.YarnConvertor;
import com.bytedance.primus.utils.FileSystemUtils;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class YarnApplicationMaster extends ApplicationMaster {

  private static final int STOP_NM_CLIENT_TIMEOUT = 300;

  private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMaster.class);

  @Getter
  private final ContainerId containerId;
  @Getter
  private final ApplicationAttemptId appAttemptId;

  // YARN components
  private final AMRMClient<ContainerRequest> amRMClient;
  private final NMClient nmClient;

  public YarnApplicationMaster(PrimusConf primusConf, ContainerId containerId) throws Exception {
    super(
        YarnApplicationMaster.class.getName(),
        createAMContext(primusConf, containerId)
    );

    // YARN components
    this.containerId = containerId;
    this.appAttemptId = containerId.getApplicationAttemptId();

    Configuration yarnConfiguration = YarnConvertor.loadYarnConfiguration(primusConf);

    LOG.info("Create YARN ResourceManager client");
    this.amRMClient = AMRMClient.createAMRMClient();
    this.amRMClient.init(yarnConfiguration);
    this.amRMClient.start();

    LOG.info("Create YARN NodeManager client");
    this.nmClient = NMClient.createNMClient();
    this.nmClient.init(yarnConfiguration);
    this.nmClient.start();

    // Upload local resources
    LOG.info("Upload local resources");
    Set<String> distributedUris = new HashSet<>();
    Set<String> distributedNames = new HashSet<>();
    Map<String, LocalResource> localResources = uploadLocalResources(
        context.getApplicationMeta(),
        distributedUris,
        distributedNames
    );

    // Finalize ApplicationMaster initialization
    RoleInfoManager roleInfoManager = new RoleInfoManager(context, new YarnRoleInfoFactory());
    context.finalize(
        this,
        new MonitorInfoProviderImpl(context.getApplicationMeta(), containerId),
        roleInfoManager,
        new FairContainerManager(context, amRMClient, roleInfoManager),
        new YarnContainerLauncher(
            context,
            appAttemptId,
            amRMClient,
            nmClient,
            yarnConfiguration,
            localResources)
    );

    context
        .getCorePrimusServices()
        .forEach(this::addService);
  }

  private static AMContext createAMContext(
      PrimusConf primusConf,
      ContainerId containerId
  ) throws Exception {

    LOG.info("Create PrimusApplicationMeta");
    String username = System.getenv().get(Environment.USER.name());
    Path stagingDir = new Path(System.getenv().get(STAGING_DIR_KEY));

    String nodeId = String.format("%s:%s",
        System.getenv().get(Environment.NM_HOST.name()),
        System.getenv().get(Environment.NM_PORT.name())
    );

    PrimusApplicationMeta applicationMeta = new PrimusApplicationMeta(
        primusConf,
        primusConf.getRuntimeConf().getYarnCommunityConf().getPrimusUiConf(),
        username,
        containerId.getApplicationAttemptId().getApplicationId().toString(),
        containerId.getApplicationAttemptId().getAttemptId(),
        getMaxAttempts(primusConf),
        stagingDir,
        0, // Random executorTrackerPort
        nodeId
    );

    LOG.info("Create AMContext");
    return new AMContext(applicationMeta, 0 /* Random executorTrackerPort by system */);
  }

  private static int getMaxAttempts(PrimusConf primusConf) {
    Configuration yarnConfiguration = YarnConvertor
        .loadYarnConfiguration(primusConf);

    return IntegerUtils.ensurePositiveOrDefault(
        primusConf.getMaxAppAttempts(),
        yarnConfiguration.getInt(
            YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS
        ));
  }

  @Override
  protected void abort() {
    LOG.info("Abort application");
    try {
      // Save history
      PrimusMetrics.TimerMetric saveHistoryLatency = PrimusMetrics
          .getTimerContextWithAppIdTag("am.save_history.latency");

      saveHistory();
      context.logTimelineEvent(
          PRIMUS_APP_SAVE_HISTORY.name(),
          String.valueOf(saveHistoryLatency.stop()));

      // Stop Primus application
      PrimusMetrics.TimerMetric stopComponentLatency = PrimusMetrics
          .getTimerContextWithAppIdTag("am.stop_component.latency");

      stop();
      context.logTimelineEvent(
          PRIMUS_APP_STOP_COMPONENT.name(),
          String.valueOf(stopComponentLatency.stop()));

      // Clean up Primus staging directory
      cleanup();

      // Unregister YARN Components
      unregisterApp();

      PrimusMetrics.TimerMetric stopNMLatency = PrimusMetrics.getTimerContextWithAppIdTag(
          "am.stop_nm.latency");

      stopNMClientWithTimeout(nmClient);
      amRMClient.stop();
      context.logTimelineEvent(PRIMUS_APP_STOP_NM.name(), String.valueOf(stopNMLatency.stop()));

      // Tear down
      context.stop();

    } catch (Exception e) {
      LOG.warn("Failed to abort application", e);
    }
  }

  private void unregisterApp() {
    try {
      if (needUnregister) {
        amRMClient.unregisterApplicationMaster(
            YarnConvertor.toYarnFinalApplicationStatus(finalStatus),
            unregisterDiagnostics,
            context.getMonitorInfoProvider().getHistoryTrackingUrl()
        );
      }
    } catch (YarnException | IOException e) {
      LOG.error("Failed to unregister application", e);
    }
  }

  private void stopNMClientWithTimeout(NMClient nodeManagerClient) {
    try {
      ListenableFuture<?> closeFuture =
          MoreExecutors
              .listeningDecorator(
                  Executors.newSingleThreadExecutor(
                      new ThreadFactoryBuilder().setDaemon(true).build()))
              .submit(() -> nodeManagerClient.stop());

      closeFuture.addListener(
          () -> LOG.info("nmClient stopped."),
          MoreExecutors.newDirectExecutorService());
      // Yarn default heartbeat interval = 600s
      closeFuture.get(STOP_NM_CLIENT_TIMEOUT, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      LOG.error("Stop nmClient TimeoutException in " + STOP_NM_CLIENT_TIMEOUT + "s", e);
    } catch (Exception e) {
      LOG.error("Error when stop nmClient in {}s", STOP_NM_CLIENT_TIMEOUT);
    }
  }

  private static Map<String, LocalResource> uploadLocalResources(
      PrimusApplicationMeta applicationMeta,
      Set<String> distributedUris,
      Set<String> distributedNames
  ) throws IOException, URISyntaxException {
    Map<String, LocalResource> localResources = new HashMap<>();
    addToResource(applicationMeta, distributedUris, distributedNames,
        PRIMUS_CONF_PATH + "/" + LOG4J_PROPERTIES,
        LOG4J_PROPERTIES,
        PRIMUS_CONF_PATH,
        localResources);
    addToResource(applicationMeta, distributedUris, distributedNames,
        PRIMUS_CONF_PATH + "/" + PRIMUS_CONF,
        PRIMUS_CONF,
        PRIMUS_CONF_PATH,
        localResources);
    addToResource(applicationMeta, distributedUris, distributedNames,
        PRIMUS_JAR_PATH + "/" + PRIMUS_JAR,
        PRIMUS_JAR,
        PRIMUS_JAR_PATH,
        localResources);
    for (String pathStr : applicationMeta.getPrimusConf().getFilesList()) {
      addToResource(applicationMeta, distributedUris, distributedNames,
          pathStr,
          null,
          null,
          localResources
      );
    }
    return localResources;
  }

  private static void addToResource(
      PrimusApplicationMeta applicationMeta,
      Set<String> distributedUris,
      Set<String> distributedNames,
      String filename,
      String destName,
      String targetDir,
      Map<String, LocalResource> localResources
  ) throws IOException, URISyntaxException {
    URI localURI = FileSystemUtils.resolveURI(filename.trim());
    if (!FileSystemUtils.addDistributedUri(localURI, distributedUris, distributedNames)) {
      return;
    }

    FileSystem fs = applicationMeta.getHadoopFileSystem();
    Path path = FileSystemUtils.getQualifiedLocalPath(localURI, fs.getConf());
    String linkname = FileSystemUtils.buildLinkname(path, localURI, destName, targetDir);
    if (!localURI.getScheme().equals(HDFS_SCHEME)) {
      path = new Path(
          fs.getUri().toString() + applicationMeta.getStagingDir() + "/" + linkname);
    }

    FileSystemUtils.addResource(fs, path, linkname, localResources);
  }
}
