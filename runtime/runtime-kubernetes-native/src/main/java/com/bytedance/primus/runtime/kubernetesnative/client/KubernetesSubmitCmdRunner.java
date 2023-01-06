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

package com.bytedance.primus.runtime.kubernetesnative.client;

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_ID_LABEL_NAME;
import static com.bytedance.primus.utils.PrimusConstants.LOG4J_PROPERTIES;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF_PATH;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CORE_TARGET_KEY;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_HOME_ENV_KEY;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR_PATH;

import com.bytedance.primus.client.ClientCmdRunner;
import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.util.RuntimeUtils;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.PrimusDriverPod;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.PrimusPodContext;
import com.bytedance.primus.runtime.kubernetesnative.common.utils.StorageHelper;
import com.bytedance.primus.runtime.kubernetesnative.runtime.monitor.MonitorInfoProviderImpl;
import com.bytedance.primus.utils.ConfigurationUtils;
import com.bytedance.primus.utils.PrimusConstants;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesSubmitCmdRunner implements ClientCmdRunner {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesSubmitCmdRunner.class);
  private static final String DEFAULT_CONFIG_FILENAME = "default.conf";

  private final PrimusConf primusConf;
  private final String appId;
  private final StorageHelper defaultFileSystem;

  public KubernetesSubmitCmdRunner(PrimusConf userPrimusConf) throws Exception {
    // Preprocess configurations
    primusConf = getMergedPrimusConf(userPrimusConf);
    LOG.info("Merged primus conf: {}", primusConf);

    // Generate application ID
    appId = generateAppId(primusConf);
    LOG.info("Generated Primus application ID: {}", appId);

    // Compute the staging direction path
    Path stagingDir = new Path(primusConf.getStagingDir(), appId);
    LOG.info("Staging Directory: {}", stagingDir);

    // Set up the default filesystem
    defaultFileSystem = new StorageHelper(
        RuntimeUtils.loadHadoopFileSystem(primusConf),
        stagingDir
    );
  }

  // TODO: Merge the configuration in the main function, so PrimusConf can be checked and then immutable throughout
  //  the entire Primus Application. Also other sub-command in the future can be benefited from the
  //  design as well.
  private PrimusConf getMergedPrimusConf(PrimusConf userPrimusConf) throws IOException {
    // Init builder
    PrimusConf.Builder builder = PrimusConf.newBuilder();

    // Try loading default configuration
    Path defaultConfFilePath = new Path(getConfDir(), DEFAULT_CONFIG_FILENAME);
    File defaultConfFile = new File(defaultConfFilePath.toString());
    if (defaultConfFile.exists()) {
      LOG.info("Loading default config from {}", defaultConfFilePath);
      PrimusConf dataCenterConf = ConfigurationUtils.load(defaultConfFilePath.toString());
      builder.mergeFrom(dataCenterConf);
    } else {
      LOG.warn("Missing default config file at {}", defaultConfFilePath);
    }

    // Override with userPrimusConf
    return builder
        .mergeFrom(userPrimusConf)
        .build();
  }

  @Override
  public void run(boolean waitAppCompletion) throws Exception {
    // Upload local resources
    uploadLocalResources();

    // Create and submit PrimusDriverPod
    PrimusPodContext primusPodContext = new PrimusPodContext(
        appId, defaultFileSystem.getApplicationStagingDir(), primusConf);

    LOG.info("Submission information, User: {}, Job: {}, AppName: {}, StagingDir: {}",
        primusPodContext.getUser(), primusConf.getName(),
        appId, defaultFileSystem.getApplicationStagingDir());

    PrimusDriverPod driverPod = new PrimusDriverPod(
        primusPodContext,
        primusConf.getRuntimeConf().getKubernetesNativeConf().getDriverPodConf()
    );

    LOG.info("Starting driver pod: {}", driverPod.getKubernetesPod());
    driverPod.submit();

    // Finalize submission
    printDebugInfo(driverPod);

    LOG.info("Pods after Create Pod!");
    if (!waitAppCompletion) {
      LOG.info("waitAppCompletion=false, so exit");
      System.exit(0);
    }

    doWaitAppCompletion(driverPod);
  }

  private void doWaitAppCompletion(PrimusDriverPod primusDriverPod) throws Exception {
    LOG.info("waitAppCompletion=true, waiting app exited.....");
    Sleeper.loopWithoutInterruptedException(
        Duration.ofSeconds(10),
        () -> {
          String status = primusDriverPod.getAMPodStatus();
          LOG.info("current driver pod status: {}", status);
          return "Succeeded".equalsIgnoreCase(status) || "Failed".equalsIgnoreCase(status);
        }
    );
  }

  /**
   * show debug info for k8s application 1: tracking url. 2: k8s log command.
   */
  private void printDebugInfo(PrimusDriverPod driverPod) {
    String namespace = Optional.ofNullable(driverPod)
        .map(PrimusDriverPod::getKubernetesPod)
        .map(V1Pod::getMetadata)
        .map(V1ObjectMeta::getNamespace)
        .orElse("default");

    String driverPodName = Optional.ofNullable(driverPod)
        .map(PrimusDriverPod::getKubernetesPod)
        .map(V1Pod::getMetadata)
        .map(V1ObjectMeta::getName)
        .orElseThrow(() -> new PrimusRuntimeException("Missing driver pod!"));

    LOG.info("=================================================================");
    LOG.info("Primus Application ID: {}", appId);
    LOG.info("Tracking URL: {}",
        MonitorInfoProviderImpl.getPreflightAmTrackingUrl(
            primusConf, appId, namespace, driverPodName));
    LOG.info("History tracking URL: {}",
        MonitorInfoProviderImpl.getPreflightHistoryTrackingUrl(
            primusConf, appId, namespace, driverPodName));
    LOG.info("Kubernetes logs command: {}",
        String.format("kubectl -n %s logs --tail=-1 -l %s=%s", namespace,
            PRIMUS_APP_ID_LABEL_NAME, appId));
    LOG.info("Kubernetes list pod command: {}",
        String.format("kubectl -n %s get pods -l %s=%s", namespace,
            PRIMUS_APP_ID_LABEL_NAME, appId));
    LOG.info("=================================================================");
  }

  // TODO: Refactor to make the upload logic more structured, also collecting all the files
  //  before uploading might be a good idea, as it makes the code easier to test.
  private void uploadLocalResources() throws IOException {
    // PrimusConf
    LOG.info("uploading primus configuration...");
    defaultFileSystem.writeConfigFile(primusConf);

    // Log4j properties
    LOG.info("uploading {}", LOG4J_PROPERTIES);
    defaultFileSystem.addToLocalResource(
        new Path(getConfDir(), LOG4J_PROPERTIES),
        PRIMUS_CONF_PATH);

    // Env preparatory
    LOG.info("uploading {}", KubernetesConstants.CONTAINER_SCRIPT_PREPARE_ENV_FILENAME);
    defaultFileSystem.addToLocalResource(
        new Path(getConfDir(), KubernetesConstants.CONTAINER_SCRIPT_PREPARE_ENV_FILENAME),
        PRIMUS_CONF_PATH);

    // Primus Jar
    LOG.info("uploading {}", PRIMUS_JAR_PATH);
    String primusHomeDir = System.getenv().get(PRIMUS_HOME_ENV_KEY);
    String primusCoreTargetDir = System.getenv().get(PRIMUS_CORE_TARGET_KEY);
    Path primusJarDir = Strings.isNullOrEmpty(primusCoreTargetDir)
        ? new Path(primusHomeDir, "jars")
        : new Path(primusCoreTargetDir);
    defaultFileSystem.addToLocalResource(new Path(primusJarDir, PRIMUS_JAR), PRIMUS_JAR_PATH);

    // Kubernetes Native files!
    defaultFileSystem.addToLocalResources(getKubernetesNativeLocalResourcePaths());

    // User Specified files
    String[] files = primusConf.getFilesList().toArray(new String[0]);
    defaultFileSystem.addToLocalResources(StorageHelper.newPaths(files));
  }

  private static String generateAppId(PrimusConf primusConf) {
    return StringUtils.ensure(
        primusConf.getRuntimeConf().getKubernetesNativeConf().getApplicationIdOverride(),
        "primus" + "-" + UUID
            .nameUUIDFromBytes(Longs.toByteArray(System.currentTimeMillis()))
            .toString()
            .toLowerCase()
            .replaceAll("-", "")
            .replaceAll("\\.", "-")
    );
  }

  // Local resources needed for Kubernetes Native (HDFS + KubeConfig + Container scripts)
  private Path[] getKubernetesNativeLocalResourcePaths() {
    // Container scripts
    Path containerScriptDir = new Path(
        System.getenv(PrimusConstants.PRIMUS_SBIN_DIR_ENV_KEY),
        KubernetesConstants.CONTAINER_SCRIPT_DIR_PATH
    );

    ArrayList<Path> ret = Arrays.stream(new String[]{
            KubernetesConstants.CONTAINER_SCRIPT_START_DRIVER_FILENAME,
            KubernetesConstants.CONTAINER_SCRIPT_START_EXECUTOR_FILENAME,
        })
        .map(filename -> new Path(containerScriptDir, filename))
        .collect(Collectors.toCollection(ArrayList::new));

    return ret.toArray(new Path[]{});
  }
}
