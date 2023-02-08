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

package com.bytedance.primus.runtime.yarncommunity.client;

import static com.bytedance.primus.utils.PrimusConstants.HDFS_SCHEME;
import static com.bytedance.primus.utils.PrimusConstants.LOG4J_PROPERTIES;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF_PATH;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_HOME_ENV_KEY;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR_PATH;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_VERSION_ENV_KEY;
import static com.bytedance.primus.utils.PrimusConstants.STAGING_DIR_KEY;

import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.client.ClientCmdRunner;
import com.bytedance.primus.common.util.RuntimeUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.yarncommunity.am.YarnApplicationMasterMain;
import com.bytedance.primus.runtime.yarncommunity.utils.YarnConvertor;
import com.bytedance.primus.utils.ConfigurationUtils;
import com.bytedance.primus.utils.ProtoJsonConverter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class YarnSubmitCmdRunner implements ClientCmdRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnSubmitCmdRunner.class);

  private static final FsPermission fsPermissionTemp = new FsPermission("777");
  private static final int YARN_AM_CONTAINER_PRIORITY = 0;
  private static final String DEFAULT_PRIMUS_CONF_FILENAME = "default.conf";

  private final PrimusConf userPrimusConf;
  private final YarnConfiguration yarnConf;

  private Path stagingDir;
  private final URI defaultFsUri;
  private final FileSystem dfs;

  private final Map<String, LocalResource> cacheFiles = new HashMap<>();
  private final Set<String> distributedUris = new HashSet<>();
  private final Set<String> distributedNames = new HashSet<>();

  public YarnSubmitCmdRunner(PrimusConf primusConf) throws Exception {
    userPrimusConf = primusConf;
    yarnConf = new YarnConfiguration(YarnConvertor.loadYarnConfiguration(primusConf));

    dfs = RuntimeUtils.loadHadoopFileSystem(primusConf);
    defaultFsUri = dfs.getUri();
  }

  @Override
  public void run(boolean waitAppCompletion) throws Exception { // TODO: Declutter this function
    PrimusConf primusConf = getMergedPrimusConf(userPrimusConf);

    // Create YARN Client
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);
    yarnClient.start();

    // Create YARN application
    YarnClientApplication app = yarnClient.createApplication();
    LOGGER.info("YARN GetNewApplicationResponse: {}", app.getNewApplicationResponse());

    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    stagingDir = new Path(primusConf.getStagingDir() + "/" + appId);
    LOGGER.info("staging dir is:" + stagingDir);

    addToLocalResource(primusConf); // TODO: Check the side effect of this function
    LOGGER.info("writing primus configuration...");

    appContext.setApplicationName(primusConf.getName());
    appContext.setApplicationType("PRIMUS");
    appContext.setAMContainerSpec(genAmContainerSpec(primusConf, yarnConf));
    appContext.setQueue(primusConf.getQueue());
    appContext.setPriority(genPriority(primusConf));
    appContext.setMaxAppAttempts(primusConf.getMaxAppAttempts());
    appContext.setApplicationTags(new HashSet<>(primusConf
        .getRuntimeConf()
        .getYarnCommunityConf()
        .getApplicationTagsList()));
    appContext.setAMContainerResourceRequest(genAMContainerResourceRequest(primusConf));

    LOGGER.info("Submitting Yarn application: (id: {})", appId);
    yarnClient.submitApplication(appContext);
    long scheduleStart = System.currentTimeMillis();

    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    LOGGER.info("Tracking URL: " + appReport.getTrackingUrl());
    if (!waitAppCompletion) {
      LOGGER.info("waitAppCompletion=false, so exit");
      System.exit(0);
    }

    YarnApplicationState appState = appReport.getYarnApplicationState();
    long lastPrintProgress = System.currentTimeMillis();
    double lastProgress = 0.0;
    boolean running = false;
    while (appState != YarnApplicationState.FINISHED
        && appState != YarnApplicationState.KILLED
        && appState != YarnApplicationState.FAILED) {
      Thread.sleep(1000);
      appReport = yarnClient.getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
      if (!running && appState == YarnApplicationState.RUNNING) {
        long scheduleTookMs = System.currentTimeMillis() - scheduleStart;
        LOGGER.info("Training successfully started. Scheduling took " + scheduleTookMs + " ms.");
        running = true;
      }
      if (System.currentTimeMillis() - lastPrintProgress > 5000
          && appReport.getProgress() - lastProgress >= 0.01) {
        LOGGER.info("State: " + appState + "  Progress: " + (appReport.getProgress() * 100) + "%");
        lastPrintProgress = System.currentTimeMillis();
        lastProgress = appReport.getProgress();
      }
    }

    appReport = yarnClient.getApplicationReport(appId);
    LOGGER.info("Application " + appId + " finished with"
        + " state " + appState + " at "
        + new SimpleDateFormat("yyyy-MM-dd HH:mm")
        .format(new Date(appReport.getFinishTime())));
    LOGGER.info("Final Application Status: {}", appReport.getFinalApplicationStatus());
    if (appReport.getFinalApplicationStatus() != FinalApplicationStatus.SUCCEEDED) {
      System.exit(ApplicationExitCode.FAIL_ATTEMPT.getValue());
    }
  }

  private PrimusConf getMergedPrimusConf(PrimusConf userConf) throws IOException {
    String defaultConfigPath = getConfDir() + "/" + DEFAULT_PRIMUS_CONF_FILENAME;
    PrimusConf defaultConf = ConfigurationUtils.load(defaultConfigPath);
    return PrimusConf.newBuilder()
        .mergeFrom(defaultConf)
        .mergeFrom(userConf)
        .build();
  }

  private String buildConfigFile(PrimusConfOuterClass.PrimusConf primusConf) throws IOException {
    String configPath = defaultFsUri.toString() + stagingDir
        + "/" + PRIMUS_CONF_PATH + "/" + PRIMUS_CONF;
    Path configFile = new Path(configPath);
    try (FSDataOutputStream out = dfs.create(configFile)) {
      String jsonFormat = ProtoJsonConverter.getJsonStringWithDefaultValueFields(primusConf);
      out.write(jsonFormat.getBytes());
      out.close();
      return configPath;
    } catch (IOException e) {
      throw new IOException("Failed to write config", e);
    }
  }

  private void addToLocalResource(String file)
      throws IOException, URISyntaxException {
    addToLocalResource(file, null);
  }

  private void addToLocalResource(String file, String destName)
      throws IOException, URISyntaxException {
    addToLocalResource(file, destName, null);
  }

  private void addToLocalResource(
      String file, String destName, String targetDir
  ) throws IOException, URISyntaxException {
    String trimmedFile = file.trim();
    URI localURI = HdfsUtil.resolveURI(trimmedFile);
    if (!HdfsUtil.addDistributedUri(localURI, distributedUris, distributedNames)) {
      return;
    }

    Path path = HdfsUtil.getQualifiedLocalPath(localURI, yarnConf);
    String linkname = HdfsUtil.buildLinkname(path, localURI, destName, targetDir);
    if (!localURI.getScheme().equals(HDFS_SCHEME)) {
      Path dst = new Path(defaultFsUri.toString() + stagingDir + "/" + linkname);
      LOGGER.info("Copy local file[{}] to dfs file[{}]", linkname, dst);
      dfs.copyFromLocalFile(false, false, path, dst);
      dfs.setPermission(dst, fsPermissionTemp);
      path = dst;
    }

    HdfsUtil.addResource(dfs, path, linkname, cacheFiles);
  }

  // TODO: Make this function static
  private Map<String, String> buildEnvironments(Configuration conf, PrimusConf primusConf) {
    Map<String, String> env = new java.util.HashMap<>();
    StringBuilder cpath = new StringBuilder()
        .append(ApplicationConstants.Environment.CLASSPATH.$$())
        .append(":")
        .append(PRIMUS_JAR_PATH + "/*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      cpath.append(":").append(c.trim());
    }
    env.put("CLASSPATH", cpath.toString());
    env.put(STAGING_DIR_KEY, stagingDir.toString());
    env.putAll(primusConf.getEnvMap());
    env.putAll(primusConf.getScheduler().getEnvMap());

    env.put(PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY, String.valueOf(new Date().getTime()));
    String version = System.getenv(PRIMUS_VERSION_ENV_KEY);
    if (version == null) {
      version = "can not get version from environment";
    }
    env.put(PRIMUS_VERSION_ENV_KEY, version);
    LOGGER.info("Primus Version: " + version);
    return env;
  }

  private static String buildCommand(PrimusConf primusConf) {
    // Raw arguments
    ArrayList<String> rawVargs = new ArrayList<>();

    rawVargs.add("$JAVA_HOME/bin/java");
    Path childTmpDir = new Path(ApplicationConstants.Environment.PWD.$(),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    rawVargs.add("-Djava.io.tmpdir=" + childTmpDir);
    rawVargs.add("-Xmx" + primusConf.getScheduler().getJvmMemoryMb() + "M");
    rawVargs.add("-Dlog4j2.configurationFile=" + PRIMUS_CONF_PATH + "/" + LOG4J_PROPERTIES);
    rawVargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    rawVargs.add(YarnApplicationMasterMain.class.getName());
    rawVargs.add("--config=" + PRIMUS_CONF_PATH + "/" + PRIMUS_CONF);
    rawVargs.add("1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    rawVargs.add("2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Concatenated
    return String.join(" ", rawVargs);
  }

  private void addToLocalResource(PrimusConf primusConf) throws IOException, URISyntaxException {
    String configPath = buildConfigFile(primusConf);
    addToLocalResource(configPath, PRIMUS_CONF, PRIMUS_CONF_PATH);

    // Primus
    String primusHomeDir = System.getenv().get(PRIMUS_HOME_ENV_KEY);
    if (primusHomeDir != null) {
      addToLocalResource(
          new Path(primusHomeDir, "jars/" + PRIMUS_JAR).toString(),
          PRIMUS_JAR, PRIMUS_JAR_PATH);
      addToLocalResource(
          new Path(getConfDir(), LOG4J_PROPERTIES).toString(),
          LOG4J_PROPERTIES, PRIMUS_CONF_PATH);
    }

    // General Files
    for (String file : primusConf.getFilesList()) {
      addToLocalResource(file);
    }
  }

  private ContainerLaunchContext genAmContainerSpec(
      PrimusConf primusConf,
      YarnConfiguration yarnConf
  ) {
    ContainerLaunchContext amContainerSpec = Records.newRecord(ContainerLaunchContext.class);
    amContainerSpec.setLocalResources(cacheFiles);
    amContainerSpec.setEnvironment(buildEnvironments(yarnConf, primusConf));
    amContainerSpec.setCommands(Collections.singletonList(buildCommand(primusConf)));
    return amContainerSpec;
  }

  private ResourceRequest genAMContainerResourceRequest(PrimusConf primusConf) {
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(primusConf.getScheduler().getMemoryMb());
    capability.setVirtualCores(primusConf.getScheduler().getVcores());
    LOGGER.info("AM Container resource capability: {}", capability);

    ResourceRequest resourceRequestPB = new ResourceRequestPBImpl();
    resourceRequestPB.setCapability(capability);
    resourceRequestPB.setPriority(Priority.newInstance(YARN_AM_CONTAINER_PRIORITY));
    resourceRequestPB.setNumContainers(1);
    resourceRequestPB.setResourceName(ResourceRequest.ANY);
    LOGGER.info("AM Container Request: {}", resourceRequestPB);

    return resourceRequestPB;
  }

  private Priority genPriority(PrimusConf primusConf) {
    Priority priority = RecordFactoryProvider
        .getRecordFactory(null)
        .newRecordInstance(Priority.class);
    priority.setPriority(primusConf.
        getRuntimeConf()
        .getYarnCommunityConf()
        .getPriority());
    return priority;
  }
}
