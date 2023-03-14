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

package com.bytedance.primus.am;

import com.bytedance.blacklist.BlacklistTracker;
import com.bytedance.blacklist.BlacklistTrackerImpl;
import com.bytedance.primus.am.apiserver.DataController;
import com.bytedance.primus.am.apiserver.DataSavepointController;
import com.bytedance.primus.am.apiserver.JobController;
import com.bytedance.primus.am.apiserver.NodeAttributeController;
import com.bytedance.primus.am.container.ContainerLauncher;
import com.bytedance.primus.am.container.ContainerLauncherEvent;
import com.bytedance.primus.am.container.ContainerLauncherEventType;
import com.bytedance.primus.am.container.ContainerManager;
import com.bytedance.primus.am.container.ContainerManagerEvent;
import com.bytedance.primus.am.container.ContainerManagerEventType;
import com.bytedance.primus.am.controller.SuspendManager;
import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.am.datastream.DataStreamManagerEvent;
import com.bytedance.primus.am.datastream.DataStreamManagerEventType;
import com.bytedance.primus.am.eventlog.ApiServerExecutorStateUpdateListener;
import com.bytedance.primus.am.eventlog.EventLoggingListener;
import com.bytedance.primus.am.eventlog.ExecutorCompleteEvent;
import com.bytedance.primus.am.eventlog.ExecutorEventType;
import com.bytedance.primus.am.eventlog.ExecutorStartEvent;
import com.bytedance.primus.am.eventlog.StatusEventLoggingListener;
import com.bytedance.primus.am.eventlog.StatusEventType;
import com.bytedance.primus.am.eventlog.StatusEventWrapper;
import com.bytedance.primus.am.failover.FailoverPolicyManager;
import com.bytedance.primus.am.failover.FailoverPolicyManagerEvent;
import com.bytedance.primus.am.failover.FailoverPolicyManagerEventType;
import com.bytedance.primus.am.master.Master;
import com.bytedance.primus.am.master.MasterContext;
import com.bytedance.primus.am.progress.ProgressManager;
import com.bytedance.primus.am.progress.ProgressManagerFactory;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.role.RoleInfoManagerEvent;
import com.bytedance.primus.am.role.RoleInfoManagerEventType;
import com.bytedance.primus.am.schedule.SchedulePolicyManager;
import com.bytedance.primus.am.schedule.SchedulePolicyManagerEvent;
import com.bytedance.primus.am.schedule.SchedulePolicyManagerEventType;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerContainerCompletedEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEventType;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorStateChangeEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorStateChangeEventType;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.client.DefaultClient;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.models.Data;
import com.bytedance.primus.apiserver.records.DataSpec;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.service.ApiServer;
import com.bytedance.primus.common.child.ChildLauncher;
import com.bytedance.primus.common.child.ChildLauncherEvent;
import com.bytedance.primus.common.child.ChildLauncherEventType;
import com.bytedance.primus.common.event.AsyncDispatcher;
import com.bytedance.primus.common.event.Event;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.common.service.Service;
import com.bytedance.primus.common.util.IntegerUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.BlacklistConfig;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.monitor.MonitorInfoProvider;
import com.bytedance.primus.utils.ResourceUtils;
import com.bytedance.primus.utils.timeline.NoopTimelineLogger;
import com.bytedance.primus.utils.timeline.TimelineLogger;
import com.bytedance.primus.webapp.CompleteApplicationServlet;
import com.bytedance.primus.webapp.HdfsStore;
import com.bytedance.primus.webapp.StatusServlet;
import com.bytedance.primus.webapp.SuccessDataStreamServlet;
import com.bytedance.primus.webapp.SuspendServlet;
import com.bytedance.primus.webapp.SuspendStatusServlet;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServlet;
import lombok.Getter;
import org.apache.hadoop.http.HttpServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hosting both the states and the internal core components, AMContext serves as the top-level
 * component representing primus-core on application master. Notably, since AMContext also hosts
 * components provided by runtime, completing the construction of an AMContext requires manually
 * invoking `finalize()` after constructors.
 */
public class AMContext {

  private static final Logger LOG = LoggerFactory.getLogger(AMContext.class);

  @Getter
  private final PrimusApplicationMeta applicationMeta;

  // Application status
  @Getter
  private final Date startTime = new Date();
  @Getter
  private Date finishTime;
  @Getter
  private FinalApplicationStatus finalStatus;
  @Getter
  private String diagnostic;
  @Getter
  private int exitCode;

  // Primus core components
  // TODO: Streamline primus-core components to signify their relationship
  private final AsyncDispatcher dispatcher = new AsyncDispatcher();
  private final AsyncDispatcher statusDispatcher = new AsyncDispatcher("statusDispatcher");

  @Getter
  private final AMService amService;
  @Getter
  private final HdfsStore hdfsStore;

  @Getter
  private final ChildLauncher masterLauncher;

  @Getter
  private final SchedulerExecutorManager schedulerExecutorManager;
  @Getter
  private final ExecutorMonitor executorMonitor;
  private final ExecutorTrackerService trackerService;
  private final BlacklistTracker blacklistTracker;

  @Getter
  private final DataStreamManager dataStreamManager;
  @Getter
  private final SuspendManager suspendManager;
  @Getter
  private final ProgressManager progressManager;

  @Getter
  private final SchedulePolicyManager schedulePolicyManager;
  @Getter
  private final FailoverPolicyManager failoverPolicyManager;

  private final ApiServer apiServer;
  @Getter
  private final CoreApi coreApi;
  @Getter
  private final JobController jobController;
  @Getter
  private final DataController dataController;
  private final NodeAttributeController nodeAttributeController;
  private final DataSavepointController dataSavepointController;
  private final ApiServerExecutorStateUpdateListener apiServerExecutorStateUpdateListener;

  @Getter
  private final HttpServer2 webAppServer;

  private final TimelineLogger timelineLogger;
  private final EventLoggingListener eventLoggingListener;
  private final StatusEventLoggingListener statusLoggingListener;
  @Getter
  private final StatusEventWrapper statusEventWrapper;

  // Runtime components (Provided by runtime implementation)
  // TODO: Streamline runtime components, where
  //  1. RoleInfoManager could be runtime agnostic
  //  2. Some of them can be combined
  @Getter
  private RoleInfoManager roleInfoManager;
  private ContainerManager containerManager;
  @Getter
  private MonitorInfoProvider monitorInfoProvider;

  public AMContext(
      PrimusApplicationMeta applicationMeta,
      int executorTrackerPort
  ) throws Exception {
    this.applicationMeta = applicationMeta;

    amService = new AMService(this);
    hdfsStore = new HdfsStore(this);

    LOG.info("Registering MasterLauncher");
    masterLauncher = new ChildLauncher(applicationMeta.getPrimusConf().getRuntimeConf());
    dispatcher.register(ChildLauncherEventType.class, masterLauncher);

    LOG.info("Registering SchedulerExecutorManager");
    schedulerExecutorManager = new SchedulerExecutorManager(this);
    dispatcher.register(SchedulerExecutorManagerEventType.class, schedulerExecutorManager);

    executorMonitor = new ExecutorMonitor(this);
    trackerService = new ExecutorTrackerService(this, executorTrackerPort);
    blacklistTracker = createBlacklistTracker(applicationMeta.getPrimusConf());

    LOG.info("Registering DataStreamManager");
    dataStreamManager = new DataStreamManager(this);
    dispatcher.register(DataStreamManagerEventType.class, dataStreamManager);

    suspendManager = new SuspendManager(dataStreamManager);
    progressManager = ProgressManagerFactory.getProgressManager(this);

    LOG.info("Registering SchedulePolicyManager");
    schedulePolicyManager = new SchedulePolicyManager(this);
    dispatcher.register(
        SchedulePolicyManagerEventType.class,
        schedulePolicyManager
    );

    LOG.info("Registering FailoverPolicyManager");
    failoverPolicyManager = new FailoverPolicyManager(this);
    dispatcher.register(
        FailoverPolicyManagerEventType.class,
        failoverPolicyManager
    );

    LOG.info("Starting Primus API Server");
    apiServer = startApiServer(applicationMeta);
    coreApi = new CoreApi(
        new DefaultClient(
            apiServer.getHostname(),
            apiServer.getPort()
        )
    );

    jobController = new JobController(this);
    dataController = new DataController(this);
    nodeAttributeController = new NodeAttributeController(this);
    dataSavepointController = new DataSavepointController(coreApi, dataStreamManager);

    apiServerExecutorStateUpdateListener = new ApiServerExecutorStateUpdateListener(this);
    dispatcher.register(
        SchedulerExecutorStateChangeEventType.class,
        apiServerExecutorStateUpdateListener
    );

    LOG.info("Starting Primus WebApp Server");
    webAppServer = startWebAppHttpServer(applicationMeta, this);

    LOG.info("Registering loggers");
    timelineLogger = new NoopTimelineLogger();
    dispatcher.register(ExecutorEventType.class, timelineLogger);

    eventLoggingListener = new EventLoggingListener(applicationMeta);
    dispatcher.register(ExecutorEventType.class, eventLoggingListener);
    dispatcher.register(ApplicationMasterEventType.class, eventLoggingListener);

    statusLoggingListener = new StatusEventLoggingListener(this);
    statusEventWrapper = new StatusEventWrapper(this, statusLoggingListener.canLogEvent());
    statusDispatcher.register(StatusEventType.class, this.statusLoggingListener);
  }

  public void finalize(
      ApplicationMaster applicationMaster,
      MonitorInfoProvider monitorInfoProvider,
      RoleInfoManager roleInfoManager,
      ContainerManager containerManager
  ) {
    finalize(
        applicationMaster,
        monitorInfoProvider,
        roleInfoManager,
        containerManager,
        null // containerLauncher
    );
  }

  public void finalize(
      ApplicationMaster applicationMaster,
      MonitorInfoProvider monitorInfoProvider,
      RoleInfoManager roleInfoManager,
      ContainerManager containerManager,
      ContainerLauncher containerLauncher // Optional
  ) {
    this.dispatcher.register(
        ApplicationMasterEventType.class,
        applicationMaster
    );

    this.roleInfoManager = roleInfoManager;
    this.dispatcher.register(
        RoleInfoManagerEventType.class,
        roleInfoManager
    );

    this.containerManager = containerManager;
    this.dispatcher.register(
        ContainerManagerEventType.class,
        containerManager
    );

    if (containerLauncher != null) {
      this.containerManager = containerManager;
      this.dispatcher.register(
          ContainerLauncherEventType.class,
          containerLauncher
      );
    }

    this.monitorInfoProvider = monitorInfoProvider;
  }

  public void stop() throws Exception {
    apiServer.stop();
    timelineLogger.shutdown();
  }

  public List<Service> getCorePrimusServices() {
    return new ArrayList<Service>() {{
      // main dispatcher
      add(dispatcher);

      add(executorMonitor);
      add(schedulerExecutorManager);
      add(trackerService);
      add(amService);
      add(dataStreamManager);
      add(suspendManager);
      add(progressManager);
      add(hdfsStore);
      add(eventLoggingListener);
      add(apiServerExecutorStateUpdateListener);
      add(masterLauncher);
      add(jobController);
      add(dataController);
      add(nodeAttributeController);
      add(dataSavepointController);
      add(containerManager);

      // status dispatcher
      add(statusDispatcher);
      add(statusLoggingListener);
    }};
  }

  public void setApplicationFinalStatus(
      FinalApplicationStatus finalApplicationStatus,
      int exitCode,
      String diagnostic
  ) {
    this.finalStatus = finalApplicationStatus;
    this.exitCode = exitCode;
    this.diagnostic = diagnostic;
    this.finishTime = new Date();
  }

  private static ApiServer startApiServer(PrimusApplicationMeta meta) throws Exception {
    ApiServer apiServer = new ApiServer(
        ResourceUtils.buildApiServerConf(meta.getPrimusConf().getApiServerConf()),
        meta.getExecutorTrackerPort()
    );
    apiServer.start();
    return apiServer;
  }

  private static HttpServer2 startWebAppHttpServer(
      PrimusApplicationMeta meta,
      AMContext context
  ) throws URISyntaxException, IOException {
    StatusServlet.setContext(context);
    CompleteApplicationServlet.setContext(context);
    SuspendServlet.setContext(context);
    SuspendStatusServlet.setContext(context);
    SuccessDataStreamServlet.setContext(context);

    HttpServer2 httpServer = new HttpServer2.Builder()
        .setFindPort(true)
        .setName("primus")
        .addEndpoint(new URI("http://0.0.0.0:" + meta.getPrimusUiConf().getWebUiPort()))
        .build();

    new HashMap<String, Class<? extends HttpServlet>>() {{
      put("/webapps/primus/status.json", StatusServlet.class);
      put("/webapps/primus/kill", CompleteApplicationServlet.class);
      put("/webapps/primus/fail", CompleteApplicationServlet.class);
      put("/webapps/primus/success", CompleteApplicationServlet.class);
      put("/webapps/primus/suspend", SuspendServlet.class);
      put("/webapps/primus/success_datastream", SuccessDataStreamServlet.class);
      put("/webapps/primus/suspend/status", SuspendStatusServlet.class);
    }}.forEach((path, clazz) ->
        httpServer.addInternalServlet(
            null, // name
            path,
            clazz,
            false // requireAuth
        ));

    httpServer.start();
    return httpServer;
  }

  private BlacklistTracker createBlacklistTracker(PrimusConf primusConf) {
    BlacklistConfig config = primusConf.getBlacklistConfig();
    if (!config.getEnabled()) {
      LOG.info("Blacklist is not enabled");
      return null;
    }

    LOG.info("Blacklist is enabled");
    int maxFailedTaskPerContainer = IntegerUtils
        .ensurePositiveOrDefault(config.getMaxFailedTaskPerContainer(), 3);
    int maxFailedContainerPerNode = IntegerUtils
        .ensurePositiveOrDefault(config.getMaxFailedContainerPerNode(), 2);
    long blacklistTimeoutMillis = IntegerUtils
        .ensurePositiveOrDefault(config.getBlacklistTimeoutMillis(), 3600000);
    int maxBlacklistContainer = IntegerUtils
        .ensurePositiveOrDefault(config.getMaxBlacklistContainer(), 50);
    int maxBlacklistNode = IntegerUtils
        .ensurePositiveOrDefault(config.getMaxBlacklistNode(), 20);

    return new BlacklistTrackerImpl(
        maxFailedTaskPerContainer,
        maxFailedContainerPerNode,
        blacklistTimeoutMillis,
        maxBlacklistContainer,
        maxBlacklistNode,
        config.getEnabled()
    );
  }

  public String getApiServerHostAddress() {
    return apiServer.getHostname();
  }

  public int getApiServerPort() {
    return apiServer.getPort();
  }

  public String getWebAppServerHostAddress() {
    return webAppServer.getConnectorAddress(0).getAddress().getHostAddress();
  }

  public int getWebAppServerPort() {
    return webAppServer.getConnectorAddress(0).getPort();
  }

  public InetSocketAddress getRpcAddress() {
    return trackerService.getRpcAddress();
  }

  public Optional<BlacklistTracker> getBlacklistTracker() {
    return blacklistTracker != null
        ? Optional.of(blacklistTracker)
        : Optional.empty();
  }

  // Loggers =======================================================================================
  // ===============================================================================================

  public void logTimelineEvent(String eventType, String eventInfo) {
    timelineLogger.logEvent(eventType, eventInfo);
  }

  public void logTimelineEvent(
      String eventType, String eventInfo,
      String infoLogFormat, Object... vargs
  ) {
    LOG.info(infoLogFormat, vargs);
    logTimelineEvent(eventType, eventInfo);
  }

  public <T extends Event> void logStatusEvent(T event) {
    try {
      if (statusLoggingListener != null && statusLoggingListener.canLogEvent()) {
        statusDispatcher.getEventHandler().handle(event);
      }
    } catch (Exception e) {
      LOG.warn("Failed to log event:", e);
    }
  }

  // Primus core events ============================================================================
  // ===============================================================================================

  // ApplicationMaster events - for updating Primus application state
  public void emitApplicationSuccessEvent(
      String exitMsg,
      int exitCode,
      long gracefulShutdownTimeoutMs
  ) {
    dispatcher
        .getEventHandler()
        .handle(new ApplicationMasterEvent(
            this, ApplicationMasterEventType.SUCCESS,
            exitMsg, exitCode, gracefulShutdownTimeoutMs
        ));
  }

  public void emitApplicationSuccessEvent(
      String exitMsg,
      int exitCode
  ) {
    dispatcher
        .getEventHandler()
        .handle(new ApplicationMasterEvent(
            this, ApplicationMasterEventType.SUCCESS,
            exitMsg, exitCode
        ));
  }

  public void emitFailApplicationEvent(String msg, int exitCode) {
    dispatcher
        .getEventHandler()
        .handle(new ApplicationMasterEvent(
            this, ApplicationMasterEventType.FAIL_APP,
            msg, exitCode));
  }

  public void emitFailAttemptEvent(String msg, int exitCode) {
    dispatcher
        .getEventHandler()
        .handle(new ApplicationMasterEvent(
            this, ApplicationMasterEventType.FAIL_ATTEMPT,
            msg, exitCode));
  }

  public void emitFailAttemptEvent(String msg, int exitCode, long gracefulShutdownTimeoutMs) {
    dispatcher
        .getEventHandler()
        .handle(new ApplicationMasterEvent(
            this, ApplicationMasterEventType.FAIL_ATTEMPT,
            msg, exitCode, gracefulShutdownTimeoutMs));
  }

  public void emitSuspendApplicationEvent(String msg) {
    dispatcher
        .getEventHandler()
        .handle(new ApplicationMasterSuspendAppEvent(this, msg));
  }

  public void emitSuspendApplicationEvent(String msg, int snapshotId) {
    dispatcher
        .getEventHandler()
        .handle(new ApplicationMasterSuspendAppEvent(this, msg, snapshotId));
  }

  public void emitResumeApplicationEvent(String msg) {
    dispatcher
        .getEventHandler()
        .handle(
            new ApplicationMasterEvent(
                this, ApplicationMasterEventType.RESUME_APP,
                msg, ApplicationExitCode.UNDEFINED.getValue()
            ));
  }

  // RoleInfoManagerEvent - for managing training roles
  // TODO: Streamline Primus core to simplify the logic for readability and atomicity.
  public void emitRoleInfoCreatedEvent(Map<String, RoleSpec> roleSpecMap) {
    dispatcher
        .getEventHandler()
        .handle(new RoleInfoManagerEvent(
            RoleInfoManagerEventType.ROLE_CREATED,
            roleSpecMap));
  }

  // TODO: Streamline Primus core to simplify the logic for readability and atomicity.
  public void emitRoleCreatedSubsequentEvents(Map<String, RoleSpec> roleSpecMap) {
    dispatcher
        .getEventHandler()
        .handle(new FailoverPolicyManagerEvent(
            FailoverPolicyManagerEventType.POLICY_CREATED,
            roleSpecMap));
    dispatcher
        .getEventHandler()
        .handle(new SchedulePolicyManagerEvent(
            SchedulePolicyManagerEventType.POLICY_CREATED,
            roleSpecMap));
    dispatcher
        .getEventHandler()
        .handle(new SchedulerExecutorManagerEvent(
            SchedulerExecutorManagerEventType.EXECUTOR_REQUEST_CREATED));
    dispatcher
        .getEventHandler()
        .handle(new ContainerManagerEvent(
            ContainerManagerEventType.CONTAINER_REQUEST_CREATED));
  }

  // TODO: Streamline Primus core to simplify the logic for readability and atomicity.
  public void emitRoleInfoUpdatedEvent(Map<String, RoleSpec> roleSpecMap) {
    dispatcher
        .getEventHandler()
        .handle(new RoleInfoManagerEvent(
            RoleInfoManagerEventType.ROLE_UPDATED,
            roleSpecMap));
  }

  // TODO: Streamline Primus core to simplify the logic for readability and atomicity.
  public void emitRoleUpdatedSubsequentEvents(Map<String, RoleSpec> roleSpecMap) {
    dispatcher
        .getEventHandler()
        .handle(new FailoverPolicyManagerEvent(
            FailoverPolicyManagerEventType.POLICY_CREATED,
            roleSpecMap));
    dispatcher
        .getEventHandler()
        .handle(new SchedulePolicyManagerEvent(
            SchedulePolicyManagerEventType.POLICY_CREATED,
            roleSpecMap));
    dispatcher
        .getEventHandler()
        .handle(new SchedulerExecutorManagerEvent(
            SchedulerExecutorManagerEventType.EXECUTOR_REQUEST_UPDATED));
    dispatcher
        .getEventHandler()
        .handle(new ContainerManagerEvent(
            ContainerManagerEventType.CONTAINER_REQUEST_UPDATED));
  }

  // SchedulerExecutorManagerEvent - for managing executor states
  public void emitExecutorExpiredEvent(ExecutorId executorId) {
    dispatcher
        .getEventHandler()
        .handle(new SchedulerExecutorManagerEvent(
            SchedulerExecutorManagerEventType.EXECUTOR_EXPIRED, executorId
        ));
  }

  public void emitExecutorKillEvent(ExecutorId executorId) {
    dispatcher
        .getEventHandler()
        .handle(new SchedulerExecutorManagerEvent(
            SchedulerExecutorManagerEventType.EXECUTOR_KILL,
            executorId));
  }

  public void emitExecutorKillForciblyEvent(ExecutorId executorId) {
    dispatcher
        .getEventHandler()
        .handle(new SchedulerExecutorManagerEvent(
            SchedulerExecutorManagerEventType.EXECUTOR_KILL_FORCIBLY,
            executorId));
  }

  public void emitExecutorKilledEvent(Container container, String exitMsg, int exitCode) {
    dispatcher
        .getEventHandler()
        .handle(new SchedulerExecutorManagerContainerCompletedEvent(
            SchedulerExecutorManagerEventType.CONTAINER_KILLED,
            container, exitCode, exitMsg)
        );
  }

  // ContainerManagerEvent - for managing containers with resource managers
  public void emitShutdownPrimusApplicationEvent(boolean forcibly) {
    dispatcher.getEventHandler().handle(forcibly
        ? new ContainerManagerEvent(ContainerManagerEventType.FORCIBLY_SHUTDOWN)
        : new ContainerManagerEvent(ContainerManagerEventType.GRACEFUL_SHUTDOWN));
  }

  public void emitContainerExpiredEvent(Container container) {
    dispatcher
        .getEventHandler()
        .handle(new ContainerManagerEvent(
            ContainerManagerEventType.EXECUTOR_EXPIRED,
            container
        ));
  }

  public void emitContainerAllocatedEvent(Container container) {
    dispatcher.getEventHandler().handle(
        new ContainerLauncherEvent(container, ContainerLauncherEventType.CONTAINER_ALLOCATED));
  }

  public void emitContainerUpdatedEvent(Container container) {
    dispatcher.getEventHandler().handle(
        new ContainerLauncherEvent(container, ContainerLauncherEventType.CONTAINER_UPDATED));
  }

  // ExecutorStatus events - for monitoring and logging purposes
  public void emitExecutorStateChangeEvent(SchedulerExecutor schedulerExecutor) {
    SchedulerExecutorStateChangeEvent event = new SchedulerExecutorStateChangeEvent(
        SchedulerExecutorStateChangeEventType.STATE_CHANGED,
        schedulerExecutor
    );
    LOG.debug("Release STATE_CHANGED event:" + event);
    dispatcher.getEventHandler().handle(event);
  }

  public void emitExecutorStartStatusEvent(String containerId, ExecutorId executorId) {
    dispatcher
        .getEventHandler()
        .handle(new ExecutorStartEvent(
            containerId,
            executorId
        ));
  }

  public void emitExecutorCompleteStatusEvent(SchedulerExecutor executor) {
    dispatcher
        .getEventHandler()
        .handle(new ExecutorCompleteEvent(this, executor));
  }

  // DataStreamManagerEvents - for managing data input
  public void emitDataInputCreatedEvent(Data data) {
    dispatcher.getEventHandler().handle(
        new DataStreamManagerEvent(
            DataStreamManagerEventType.DATA_STREAM_CREATED,
            data.getSpec(),
            data.getMeta().getVersion()));
  }

  public void emitDataInputUpdatedEvent(Data data) {
    dispatcher.getEventHandler().handle(
        new DataStreamManagerEvent(
            DataStreamManagerEventType.DATA_STREAM_UPDATE,
            data.getSpec(),
            data.getMeta().getVersion()));
  }

  public void emitSucceedDataStreamEvent(DataSpec dataSpec) {
    dispatcher
        .getEventHandler()
        .handle(new DataStreamManagerEvent(
            DataStreamManagerEventType.DATA_STREAM_SUCCEED,
            dataSpec, 0L // Set version to 0, cause event handler do not need version number
        ));
  }

  // Experimental
  public void emitChiefTrainerStartEvent(MasterContext masterContext) {
    dispatcher
        .getEventHandler()
        .handle(new ChildLauncherEvent(
            ChildLauncherEventType.LAUNCH,
            new Master(this, masterContext, dispatcher)
        ));
  }
}
