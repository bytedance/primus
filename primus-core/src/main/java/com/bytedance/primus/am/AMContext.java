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

import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_VERSION_ENV_KEY;

import com.bytedance.blacklist.BlacklistTracker;
import com.bytedance.primus.am.apiserver.DataController;
import com.bytedance.primus.am.apiserver.JobController;
import com.bytedance.primus.am.apiserver.NodeAttributeController;
import com.bytedance.primus.am.controller.SuspendManager;
import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.am.eventlog.StatusEventLoggingListener;
import com.bytedance.primus.am.eventlog.StatusEventWrapper;
import com.bytedance.primus.am.failover.FailoverPolicyManager;
import com.bytedance.primus.am.master.Master;
import com.bytedance.primus.am.progress.ProgressManager;
import com.bytedance.primus.am.psonyarn.PonyManager;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedule.SchedulePolicyManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.common.event.Dispatcher;
import com.bytedance.primus.common.event.Event;
import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.exceptions.PrimusUnsupportedException;
import com.bytedance.primus.common.model.ApplicationConstants.Environment;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.common.util.AbstractLivelinessMonitor;
import com.bytedance.primus.common.util.RuntimeUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.monitor.MonitorInfoProvider;
import com.bytedance.primus.utils.timeline.TimelineLogger;
import com.bytedance.primus.webapp.HdfsStore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Being an abstract base class, there are multiple components in AMContext not being able to be
 * initialized during the constructor. Hence, init() is created to finalize the construction.
 */
public abstract class AMContext {

  private static final Logger LOG = LoggerFactory.getLogger(AMContext.class);

  protected Map<String, String> envs;
  @Getter
  protected final PrimusConf primusConf;

  protected Dispatcher dispatcher;
  protected Dispatcher statusDispatcher;

  // Runtime Environment
  @Getter
  protected final FileSystem hadoopFileSystem; // TODO: Create Primus FileSystem interface and abstract direct dependencies on HDFS.

  // Common Components
  protected RoleInfoManager roleInfoManager;
  protected FailoverPolicyManager failoverPolicyManager;
  protected SchedulePolicyManager schedulePolicyManager;
  protected SchedulerExecutorManager schedulerExecutorManager;
  protected AbstractLivelinessMonitor monitor;
  protected ContainerId containerId;
  protected String nodeId;
  protected ApplicationAttemptId appAttemptId;
  protected String username;
  protected ProgressManager progressManager;
  protected DataStreamManager dataStreamManager;
  protected Optional<BlacklistTracker> blacklistTrackerOpt;
  protected Map<String, String> executorNodeMap;

  private InetSocketAddress rpcAddress;
  private Date startTime = new Date();
  private Date finishTime;
  private FinalApplicationStatus finalStatus;
  private HttpServer2 httpServer;
  private InetSocketAddress httpAddress;
  private HdfsStore hdfsStore;
  private PonyManager ponyManager;
  private AMService amService;
  private TimelineLogger timelineLogger;
  private Map<Long, Map<String, String>> gangSchedulerQueue;
  private CoreApi coreApi;
  private JobController jobController;
  private DataController dataController;
  private NodeAttributeController nodeAttributeController;
  private Master master;

  private SuspendManager suspendManager;

  private String version;
  private String diagnostic;
  private int exitCode;
  private StatusEventWrapper statusEventWrapper;
  private StatusEventLoggingListener statusLoggingListener;

  // Runtime Components
  @Getter
  private MonitorInfoProvider monitorInfoProvider;

  public AMContext(PrimusConf primusConf) throws IOException {
    envs = new HashMap<>(System.getenv());
    executorNodeMap = new ConcurrentHashMap<>();
    timelineLogger = null;
    gangSchedulerQueue = new HashMap<>();

    this.primusConf = primusConf;
    this.version = envs.get(PRIMUS_VERSION_ENV_KEY);
    LOG.info("PRIMUS Version: " + version);

    // Setting up runtime components
    this.hadoopFileSystem = RuntimeUtils.loadHadoopFileSystem(primusConf);
  }

  // TODO: Scan Primus components in AMContext and make them final.
  protected AMContext init(MonitorInfoProvider monitorInfoProvider) {
    this.monitorInfoProvider = monitorInfoProvider;
    return this;
  }

  public Map<String, String> getEnvs() {
    return envs;
  }

  public void setDispatcher(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public Dispatcher getStatusDispatcher() {
    return statusDispatcher;
  }

  public void setStatusDispatcher(Dispatcher statusDispatcher) {
    this.statusDispatcher = statusDispatcher;
  }

  public PrimusConf getPrimusConf() {
    return primusConf;
  }

  public void setRoleInfoManager(RoleInfoManager roleInfoManager) {
    this.roleInfoManager = roleInfoManager;
  }

  public RoleInfoManager getRoleInfoManager() {
    return roleInfoManager;
  }

  public FailoverPolicyManager getFailoverPolicyManager() {
    return failoverPolicyManager;
  }

  public void setFailoverPolicyManager(
      FailoverPolicyManager failoverPolicyManager) {
    this.failoverPolicyManager = failoverPolicyManager;
  }

  public SchedulePolicyManager getSchedulePolicyManager() {
    return schedulePolicyManager;
  }

  public void setSchedulePolicyManager(
      SchedulePolicyManager schedulePolicyManager) {
    this.schedulePolicyManager = schedulePolicyManager;
  }

  public void setSchedulerExecutorManager(SchedulerExecutorManager schedulerExecutorManager) {
    this.schedulerExecutorManager = schedulerExecutorManager;
  }

  public SchedulerExecutorManager getSchedulerExecutorManager() {
    return schedulerExecutorManager;
  }

  public AbstractLivelinessMonitor getMonitor() {
    return monitor;
  }

  public void setMonitor(AbstractLivelinessMonitor monitor) {
    this.monitor = monitor;
  }

  public ContainerId getContainerId() {
    if (containerId == null) {
      String cid = envs.get(Environment.CONTAINER_ID.name());
      if (StringUtils.isEmpty(cid)) {
        cid = System.getProperty(Environment.CONTAINER_ID.name());
      }
      containerId = ContainerId.fromString(cid);
      String nmHost = envs.get(Environment.NM_HOST.name());
      if (nmHost != null) {
        nodeId = nmHost + ":" + envs.get(Environment.NM_PORT.name());
      }
    }
    return containerId;
  }

  public ApplicationAttemptId getAppAttemptId() {
    if (appAttemptId == null) {
      appAttemptId = getContainerId().getApplicationAttemptId();
    }
    return appAttemptId;
  }

  public String getUsername() {
    if (username == null) {
      username = envs.get(Environment.USER.name());
    }
    return username;
  }

  public ProgressManager getProgressManager() {
    return progressManager;
  }

  public void setProgressManager(ProgressManager progressManager) {
    this.progressManager = progressManager;
  }

  public DataStreamManager getDataStreamManager() {
    return dataStreamManager;
  }

  public void setDataStreamManager(DataStreamManager dataStreamManager) {
    this.dataStreamManager = dataStreamManager;
  }

  public void setRpcAddress(InetSocketAddress rpcAddress) {
    this.rpcAddress = rpcAddress;
  }

  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  public Date getStartTime() {
    return startTime;
  }

  public Date getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(Date finishTime) {
    this.finishTime = finishTime;
  }

  public FinalApplicationStatus getFinalStatus() {
    return finalStatus;
  }

  public void setFinalStatus(FinalApplicationStatus finalStatus) {
    this.finalStatus = finalStatus;
  }

  public void setHttpServer(HttpServer2 httpServer) {
    this.httpServer = httpServer;
    this.httpAddress = NetUtils.getConnectAddress(httpServer.getConnectorAddress(0));
  }

  public HttpServer2 getHttpServer() {
    return httpServer;
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public HdfsStore getHdfsStore() {
    return hdfsStore;
  }

  public void setHdfsStore(HdfsStore hdfsStore) {
    this.hdfsStore = hdfsStore;
  }

  public void setBlacklistTrackerOpt(Optional<BlacklistTracker> blacklistTrackerOpt) {
    this.blacklistTrackerOpt = blacklistTrackerOpt;
  }

  public Optional<BlacklistTracker> getBlacklistTrackerOpt() {
    return blacklistTrackerOpt;
  }

  public Map<String, String> getExecutorNodeMap() {
    return executorNodeMap;
  }

  public PonyManager getPonyManager() {
    return ponyManager;
  }

  public void setPonyManager(PonyManager ponyManager) {
    this.ponyManager = ponyManager;
  }

  public AMService getAmService() {
    return amService;
  }

  public void setAmService(AMService amService) {
    this.amService = amService;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public void setTimelineLogger(TimelineLogger timelineLogger) {
    this.timelineLogger = timelineLogger;
  }

  public TimelineLogger getTimelineLogger() {
    return timelineLogger;
  }

  public Map<Long, Map<String, String>> getGangSchedulerQueue() {
    return gangSchedulerQueue;
  }

  public String getVersion() {
    return version;
  }

  public int getExecutorTrackPort() {
    return 0;
  }

  public CoreApi getCoreApi() {
    return coreApi;
  }

  public void setCoreApi(CoreApi coreApi) {
    this.coreApi = coreApi;
  }

  public JobController getJobController() {
    return jobController;
  }

  public void setJobController(JobController jobController) {
    this.jobController = jobController;
  }

  public DataController getDataController() {
    return dataController;
  }

  public void setDataController(DataController dataController) {
    this.dataController = dataController;
  }

  public NodeAttributeController getNodeAttributeController() {
    return nodeAttributeController;
  }

  public void setNodeAttributeController(NodeAttributeController nodeAttributeController) {
    this.nodeAttributeController = nodeAttributeController;
  }

  public Master getMaster() {
    return master;
  }

  public void setMaster(Master master) {
    this.master = master;
  }

  public SuspendManager getSuspendManager() {
    return suspendManager;
  }

  public void setSuspendManager(SuspendManager suspendManager) {
    this.suspendManager = suspendManager;
  }

  public String getDiagnostic() {
    return diagnostic;
  }

  public void setDiagnostic(String diagnostic) {
    this.diagnostic = diagnostic;
  }

  public int getExitCode() {
    return exitCode;
  }

  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

  public StatusEventWrapper getStatusEventWrapper() {
    return statusEventWrapper;
  }

  public void setStatusEventWrapper(StatusEventWrapper statusEventWrapper) {
    this.statusEventWrapper = statusEventWrapper;
  }

  public StatusEventLoggingListener getStatusLoggingListener() {
    return statusLoggingListener;
  }

  public void setStatusLoggingListener(
      StatusEventLoggingListener statusLoggingListener) {
    this.statusLoggingListener = statusLoggingListener;
  }

  public <T extends Event> void logStatusEvent(T event) {
    try {
      if (statusLoggingListener != null && statusLoggingListener.canLogEvent()) {
        getStatusDispatcher().getEventHandler().handle(event);
      }
    } catch (Exception e) {
      LOG.warn("Failed to log event:", e);
    }
  }

  // TODO: Make AMContext abstract, and force the implementation to explicitly setup this feature.
  // TODO: After landing the new runtime model, maybe this feature can be bypassed via not
  //  initializing an ExecutorMonitor.
  // Toggles whether ExecutorMonitor periodically updates container status changes to API server.
  public boolean needToUpdateExecutorToApiServer() {
    return false;
  }

  public Map<String, String> retrieveContainerMetric(Container container)
      throws IOException, PrimusRuntimeException, PrimusUnsupportedException {
    throw new PrimusUnsupportedException("retrieveContainerMetric is not implemented.");
  }

  public String getApplicationNameForStorage() {
    return this.getAppAttemptId().getApplicationId().toString();
  }
}
