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

import static com.bytedance.primus.utils.PrimusConstants.LOG4J_PROPERTIES;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF_PATH;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CORE_TARGET_KEY;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_HOME_ENV_KEY;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR_PATH;
import static com.bytedance.primus.utils.PrimusConstants.SYSTEM_USER_ENV_KEY;

import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceRequest;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceType;
import com.bytedance.primus.apiserver.records.ExecutorSpec;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.records.impl.ExecutorSpecImpl;
import com.bytedance.primus.apiserver.records.impl.RoleSpecImpl;
import com.bytedance.primus.client.ClientCmdRunner;
import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.util.RuntimeUtils;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusConfOuterClass.Scheduler;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesScheduler;
import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesResourceLimitConverter;
import com.bytedance.primus.runtime.kubernetesnative.common.KubernetesSchedulerConfig;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.PrimusDriverPod;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.PrimusPodContext;
import com.bytedance.primus.runtime.kubernetesnative.runtime.monitor.MonitorInfoProviderImpl;
import com.bytedance.primus.runtime.kubernetesnative.utils.StorageHelper;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final String appName;
  private final StorageHelper defaultFileSystem;

  public KubernetesSubmitCmdRunner(PrimusConf userPrimusConf) throws Exception {
    // Preprocess configurations
    // TODO: would it be a better idea that we merge the configuration in the main function?
    //  so that we can subsequently make PrimusConf immutable in the entire Primus Application. Also
    //  other sub-command in the future can be benefited from the design as well.
    primusConf = getMergedPrimusConf(userPrimusConf);
    LOG.info("Merged primus conf: {}", primusConf);

    // Generate application name
    appName = generateAppName(primusConf);
    LOG.info("Generated Kubernetes APP_NAME: {}", appName);

    // Compute the staging direction path
    Path stagingDir = new Path(primusConf.getStagingDir(), appName);
    LOG.info("Staging Directory: {}", stagingDir);

    // Set up the default filesystem
    defaultFileSystem = new StorageHelper(
        RuntimeUtils.loadHadoopFileSystem(primusConf),
        stagingDir
    );
  }

  /**
   * Merge userPrimusConf into system defaultPrimusConf if exists (overrides).
   *
   * @param userPrimusConf from user
   * @return merged PrimusConfig combined with environment default configurations.
   */
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
    return builder.mergeFrom(userPrimusConf).build();
  }

  @Override
  public void run(boolean waitAppCompletion) throws Exception {
    // Upload local resources
    uploadLocalResources();

    // Create and submit PrimusDriverPod
    PrimusPodContext primusPodContext = newPrimusDriverPodContext(
        appName, defaultFileSystem.getApplicationStagingDir(), primusConf);

    LOG.info("Current Primus submission, Owner:{}, Job:{}, AppName: {}, StagingDir: {}",
        primusPodContext.getOwner(), primusConf.getName(),
        appName, defaultFileSystem.getApplicationStagingDir());

    PrimusDriverPod driverPod = new PrimusDriverPod(
        primusPodContext,
        createResourceLimitMap(primusConf),
        primusConf.getRuntimeConf().getKubernetesNativeConf().getInitContainerConf(),
        primusConf.getRuntimeConf().getKubernetesNativeConf().getDriverContainerConf()
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

  private static Map<String, String> getDriverStartEnvironment(PrimusConf conf) {
    Map<String, String> environment = new HashMap<>();

    environment.put(
        KubernetesConstants.PRIMUS_AM_JAVA_MEMORY_XMX,
        conf.getScheduler().getJvmMemoryMb() + "m"
    );

    if (!Strings.isNullOrEmpty(conf.getScheduler().getJavaOpts())) {
      environment.put(
          KubernetesConstants.PRIMUS_AM_JAVA_OPTIONS,
          Integer.toString(conf.getScheduler().getJvmMemoryMb()));
    }

    environment.putAll(conf.getScheduler().getEnvMap());
    return environment;
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
    LOG.info("Kubernetes AM tracking URL: {}",
        MonitorInfoProviderImpl.getPreflightAmTrackingUrl(
            primusConf, appName, namespace, driverPodName));
    LOG.info("Kubernetes History tracking URL: {}",
        MonitorInfoProviderImpl.getPreflightHistoryTrackingUrl(
            primusConf, appName, namespace, driverPodName));
    LOG.info("Kubernetes logs command: {}",
        String.format("kubectl -n %s logs %s", namespace, driverPodName));
    LOG.info("Kubernetes exec command: {}",
        String.format("kubectl -n %s exec -it %s -- bash", namespace, driverPodName));
    LOG.info("Kubernetes list pod command: {}",
        String.format("kubectl -n %s get pods | grep %s", namespace, driverPodName));
    LOG.info("=================================================================");
  }

  private static Map<String, String> createResourceLimitMap(PrimusConf primusConf) {
    RoleSpec roleSpec = new RoleSpecImpl();
    ExecutorSpec executorSpec = new ExecutorSpecImpl();
    List<ResourceRequest> resourceRequests = new ArrayList<>();

    resourceRequests.add(
        ResourceRequest.newBuilder()
            .setResourceType(ResourceType.VCORES)
            .setValue(primusConf.getScheduler().getVcores())
            .build()
    );
    resourceRequests.add(
        ResourceRequest.newBuilder()
            .setResourceType(ResourceType.MEMORY_MB)
            .setValue(primusConf.getScheduler().getMemoryMb())
            .build()
    );
    if (primusConf.getScheduler().getGpuNum() != 0) {
      resourceRequests.add(
          ResourceRequest.newBuilder()
              .setResourceType(ResourceType.GPU)
              .setValue(primusConf.getScheduler().getGpuNum())
              .build());
    }

    executorSpec.setResourceRequests(resourceRequests);
    roleSpec.setExecutorSpecTemplate(executorSpec);
    Map<String, String> stringStringMap = KubernetesResourceLimitConverter
        .buildResourceLimitMap(roleSpec);
    LOG.info("Total AM ResourceLimit:" + stringStringMap);
    return stringStringMap;
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

  // TODO: unit tests
  private static String generateAppName(PrimusConf primusConf) {
    // Get the name override
    String nameOverride = Optional.of(primusConf.getScheduler())
        .map(Scheduler::getKubernetesScheduler)
        .map(KubernetesScheduler::getDriverPodNameOverride)
        .orElse(null);

    if (!Strings.isNullOrEmpty(nameOverride)) {
      return nameOverride;
    }

    // Let's generate one
    LOG.info("Missing DriverPodNameOverride in PrimusConf, generating a random application name.");
    return "primus" + "-" + UUID
        .nameUUIDFromBytes(Longs.toByteArray(System.currentTimeMillis()))
        .toString().toLowerCase()
        .replaceAll("-", "")
        .replaceAll("\\.", "-");
  }

  private static PrimusPodContext newPrimusDriverPodContext(
      String appName,
      Path stagingDir,
      PrimusConf primusConf
  ) {
    PrimusPodContext context = new PrimusPodContext();
    context.setAppName(appName);
    context.setHdfsStagingDir(stagingDir);
    context.setJobName(primusConf.getName());
    context.setOwner(StringUtils.ensure(
        primusConf.getKubernetesJobConf().getOwner(),
        System.getenv().get(SYSTEM_USER_ENV_KEY)));
    context.setSleepSecondsBeforePodExit(
        primusConf
            .getScheduler()
            .getKubernetesScheduler()
            .getRuntimeConfig()
            .getSleepSecondsBeforePodExit());
    context.setKubernetesSchedulerConfig(new KubernetesSchedulerConfig(primusConf));
    context.setKubernetesJobName(primusConf.getKubernetesJobConf().getKubernetesJobName());
    context.setJobEnvironMap(primusConf.getEnvMap()); // use for config map
    context.setDriverEnvironMap(getDriverStartEnvironment(primusConf));
    context.setRuntimeConf(primusConf.getRuntimeConf());

    return context;
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
