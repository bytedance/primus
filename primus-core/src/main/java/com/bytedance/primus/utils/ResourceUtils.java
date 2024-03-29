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

import static com.bytedance.primus.am.datastream.DataStreamManager.DEFAULT_DATA_STREAM;

import com.bytedance.primus.apiserver.client.models.Data;
import com.bytedance.primus.apiserver.client.models.Job;
import com.bytedance.primus.apiserver.proto.ApiServerConfProto;
import com.bytedance.primus.apiserver.proto.DataProto.DataSourceSpec;
import com.bytedance.primus.apiserver.proto.DataProto.DataSpec;
import com.bytedance.primus.apiserver.proto.DataProto.DataStreamSpec;
import com.bytedance.primus.apiserver.proto.DataProto.KafkaSourceSpec;
import com.bytedance.primus.apiserver.proto.ResourceProto;
import com.bytedance.primus.apiserver.proto.ResourceProto.ExecutorSpec;
import com.bytedance.primus.apiserver.proto.ResourceProto.JobSpec;
import com.bytedance.primus.apiserver.proto.ResourceProto.Plugin;
import com.bytedance.primus.apiserver.proto.ResourceProto.RoleSpec;
import com.bytedance.primus.apiserver.proto.UtilsProto;
import com.bytedance.primus.apiserver.proto.UtilsProto.CommonFailoverPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.DynamicSchedulePolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.ElasticResource;
import com.bytedance.primus.apiserver.proto.UtilsProto.EnvInputPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.ExitCodeFailoverPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.FailoverPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.GangSchedulePolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.InputPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.MaxFailurePolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceRequest;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceType;
import com.bytedance.primus.apiserver.proto.UtilsProto.RestartType;
import com.bytedance.primus.apiserver.proto.UtilsProto.SchedulePolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.ScheduleStrategy;
import com.bytedance.primus.apiserver.proto.UtilsProto.StreamingInputPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.SuccessPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.ValueRange;
import com.bytedance.primus.apiserver.records.Meta;
import com.bytedance.primus.apiserver.records.impl.DataSpecImpl;
import com.bytedance.primus.apiserver.records.impl.JobSpecImpl;
import com.bytedance.primus.apiserver.records.impl.MetaImpl;
import com.bytedance.primus.common.util.IntegerUtils;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass;
import com.bytedance.primus.proto.PrimusConfOuterClass.ApiServerConf;
import com.bytedance.primus.proto.PrimusConfOuterClass.Attribute;
import com.bytedance.primus.proto.PrimusConfOuterClass.CommonFailover;
import com.bytedance.primus.proto.PrimusConfOuterClass.Failover;
import com.bytedance.primus.proto.PrimusConfOuterClass.HybridDeploymentFailover;
import com.bytedance.primus.proto.PrimusConfOuterClass.OrderSchedulePolicy.RolePolicy;
import com.bytedance.primus.proto.PrimusConfOuterClass.PluginConfig;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusConfOuterClass.Role;
import com.bytedance.primus.proto.PrimusConfOuterClass.RoleScheduleType;
import com.bytedance.primus.proto.PrimusInput.FileConfig;
import com.bytedance.primus.proto.PrimusInput.FileConfig.Input;
import com.bytedance.primus.proto.PrimusInput.InputManager;
import com.bytedance.primus.proto.PrimusInput.InputManager.ConfigCase;
import com.bytedance.primus.proto.PrimusInput.KafkaConfig;
import com.bytedance.primus.proto.PrimusInput.KafkaConfig.Topic;
import com.bytedance.primus.proto.PrimusRuntime.YarnNeedGlobalNodesView;
import com.bytedance.primus.proto.PrimusRuntime.YarnScheduler;
import com.bytedance.primus.proto.PrimusRuntime.YarnScheduler.BatchScheduler;
import com.bytedance.primus.proto.PrimusRuntime.YarnScheduler.GangScheduler;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

  private static final int CONTAINER_EXIT_STATUS_KILLED_BY_CONTAINER_EVICTION_MANAGER = -1818;
  private static final int KILLED_BY_HTTP = -31001;

  public static Meta buildMeta(String name) {
    Meta meta = new MetaImpl();
    meta.setName(name);
    return meta;
  }

  public static Job buildJob(PrimusConf primusConf) {
    Meta jobMeta = buildMeta(primusConf.getName());
    JobSpec jobSpec = buildJobSpec(primusConf);
    Job job = new Job();
    job.setMeta(jobMeta).setSpec(new JobSpecImpl(jobSpec));
    return job;
  }

  public static JobSpec buildJobSpec(PrimusConf primusConf) {
    JobSpec.Builder builder = JobSpec.newBuilder();
    builder.putAllRoleSpecs(buildRoleSpecs(primusConf));
    return builder.build();
  }

  public static Map<String, RoleSpec> buildRoleSpecs(PrimusConf primusConf) {
    Map<String, RoleSpec> roleSpecs = new TreeMap<>();
    SchedulePolicy schedulePolicy = buildSchedulePolicy(primusConf);
    Map<String, SchedulePolicy> roleNameOperatorPolicyMap = getOperatorPolicy(primusConf);
    for (Role role : primusConf.getRoleList()) {
      RoleSpec.Builder roleBuilder = RoleSpec.newBuilder();
      roleBuilder.setReplicas(role.getNum());
      roleBuilder.setMinReplicas(role.getMinNum());
      roleBuilder.setExecutorSpecTemplate(buildExecutorSpec(role));
      if (roleNameOperatorPolicyMap.size() != 0) {
        schedulePolicy = roleNameOperatorPolicyMap.get(role.getRoleName());
      }
      roleBuilder.setSchedulePolicy(schedulePolicy);
      roleBuilder.setFailoverPolicy(buildFailoverPolicy(role.getFailover()));
      roleBuilder.setSuccessPolicy(buildSuccessPolicy(role));
      roleBuilder.setRoleScheduler(buildYarnScheduler(role.getRoleScheduler()));
      roleBuilder.setScheduleStrategy(buildScheduleStrategy(role));
      roleSpecs.put(role.getRoleName(), roleBuilder.build());
    }
    return roleSpecs;
  }

  public static UtilsProto.YarnScheduler.BatchScheduler buildBatchScheduler(
      YarnScheduler roleScheduler) {
    BatchScheduler batchScheduler = roleScheduler.getBatchScheduler();
    return UtilsProto.YarnScheduler.BatchScheduler.newBuilder()
        .setSetResourceTypeByHardConstraint(batchScheduler.getSetResourceTypeByHardConstraint())
        .build();
  }

  public static UtilsProto.YarnScheduler.GangScheduler buildGangScheduler(
      YarnScheduler roleScheduler) {
    GangScheduler gangScheduler = roleScheduler.getGangScheduler();
    return UtilsProto.YarnScheduler.GangScheduler.newBuilder()
        .setMaxWaitSeconds(gangScheduler.getMaxWaitSeconds())
        .setMaxRetryNum(gangScheduler.getMaxRetryNum())
        .build();
  }

  public static UtilsProto.YarnScheduler.FairScheduler buildFairScheduler() {
    return UtilsProto.YarnScheduler.FairScheduler.newBuilder().build();
  }

  public static UtilsProto.YarnScheduler buildYarnScheduler(YarnScheduler roleScheduler) {
    UtilsProto.YarnScheduler.Builder builder = UtilsProto.YarnScheduler.newBuilder();
    switch (roleScheduler.getSchedulerCase()) {
      case FAIR_SCHEDULER:
        builder.setFairScheduler(buildFairScheduler());
        break;
      case GANG_SCHEDULER:
        builder.setGangScheduler(buildGangScheduler(roleScheduler));
        break;
      case BATCH_SCHEDULER:
        builder.setBatchScheduler(buildBatchScheduler(roleScheduler));
        break;
      default:
        break;
    }
    if (roleScheduler.getNeedGlobalNodesView() == YarnNeedGlobalNodesView.TRUE) {
      builder.setNeedGlobalNodesView(UtilsProto.YarnScheduler.YarnNeedGlobalNodesView.TRUE);
    }
    return builder.build();
  }

  private static ScheduleStrategy buildScheduleStrategy(Role role) {
    return ScheduleStrategy.newBuilder()
        .setMaxReplicasPerNode(role.getScheduleStrategy().getMaxReplicasPerNode())
        .setExecutorDelayStartSeconds(role.getScheduleStrategy().getExecutorDelayStartSeconds())
        .setRoleCategoryValue(role.getScheduleStrategy().getRoleCategoryValue())
        .setElasticResource(
            ElasticResource.newBuilder()
                .setExtendMemRatio(
                    role.getScheduleStrategy().getElasticResource().getExtendMemRatio())
                .setExtendMemSize(
                    role.getScheduleStrategy().getElasticResource().getExtendMemSize())
                .build())
        .build();
  }

  public static SchedulePolicy buildSchedulePolicy(PrimusConf primusConf) {
    SchedulePolicy.Builder builder = SchedulePolicy.newBuilder();
    PrimusConfOuterClass.SchedulePolicy schedulePolicy =
        primusConf.getScheduler().getSchedulePolicy();
    if (schedulePolicy.hasDynamicPolicy()) {
      builder.setDynamicSchedulePolicy(DynamicSchedulePolicy.getDefaultInstance());
    } else if (schedulePolicy.hasGangPolicy()) {
      builder.setGangSchedulePolicy(GangSchedulePolicy.getDefaultInstance());
    }
    return builder.build();
  }

  public static Map<String, SchedulePolicy> getOperatorPolicy(PrimusConf primusConf) {
    Map<String, SchedulePolicy> map = new HashMap<>();
    if (primusConf.getScheduler().getSchedulePolicy().hasOrderPolicy()) {
      for (RolePolicy rolePolicy :
          primusConf.getScheduler().getSchedulePolicy().getOrderPolicy().getRolePolicyList()) {
        SchedulePolicy.Builder builder = SchedulePolicy.newBuilder();
        RoleScheduleType roleScheduleType = rolePolicy.getScheduleType();
        if (roleScheduleType == RoleScheduleType.GANG) {
          builder.setGangSchedulePolicy(GangSchedulePolicy.getDefaultInstance());
        } else if (roleScheduleType == RoleScheduleType.DYNAMIC) {
          builder.setDynamicSchedulePolicy(DynamicSchedulePolicy.getDefaultInstance());
        }
        map.put(rolePolicy.getRoleName(), builder.build());
      }
    }
    return map; // map would be empty if it's not order policy
  }

  public static ExecutorSpec buildExecutorSpec(Role role) {
    ExecutorSpec.Builder executorBuilder = ExecutorSpec.newBuilder();
    executorBuilder.addAllResourceRequests(buildResourceRequest(role));
    // TODO: Isolate JVM memory size from general JVM options
    String javaOpt = String.format("-Xmx%dm",
        IntegerUtils.ensurePositiveOrDefault(
            role.getJvmMemoryMb(),
            PrimusConstants.DEFAULT_EXECUTOR_JAVA_MEMORY_MB
        ));
    if (!role.getJavaOpts().isEmpty()) {
      javaOpt = javaOpt + " " + role.getJavaOpts();
    }
    executorBuilder.setJavaOpts(javaOpt);
    executorBuilder.setCommand(role.getCommand());
    executorBuilder.putAllEnvs(role.getEnvMap());
    executorBuilder.setInputPolicy(buildInputPolicy(role));
    executorBuilder.setIsEvaluation(role.getIsEvaluation());

    if (role.hasPluginConfig()) {
      PluginConfig pluginConfig = role.getPluginConfig();
      ResourceProto.PluginConfig.Builder pluginConfigBuilder = ResourceProto.PluginConfig
          .newBuilder();
      if (pluginConfig.getBasicPluginsCount() > 0) {
        List<Plugin> plugins = buildPluginList(pluginConfig.getBasicPluginsList());
        pluginConfigBuilder.addAllBasicPlugins(plugins);
      }
      if (pluginConfig.getExtendPluginsCount() > 0) {
        List<Plugin> plugins = buildPluginList(pluginConfig.getExtendPluginsList());
        pluginConfigBuilder.addAllExtendPlugins(plugins);
      }
      if (pluginConfig.getDisabledPluginsCount() > 0) {
        List<Plugin> plugins = buildPluginList(pluginConfig.getDisabledPluginsList());
        pluginConfigBuilder.addAllDisabledPlugins(plugins);
      }
      executorBuilder.setPluginConfig(pluginConfigBuilder);
    }
    return executorBuilder.build();
  }

  public static List<Plugin> buildPluginList(List<PrimusConfOuterClass.Plugin> plugins) {
    List<Plugin> pluginList = new ArrayList<>();
    for (PrimusConfOuterClass.Plugin plugin : plugins) {
      Plugin pluginResource = Plugin.newBuilder()
          .setName(plugin.getName())
          .setVersion(plugin.getVersion())
          .putAllParameter(plugin.getParameterMap())
          .build();
      pluginList.add(pluginResource);
    }
    return pluginList;
  }

  public static FailoverPolicy buildFailoverPolicy(Failover failover) {
    FailoverPolicy.Builder failoverPolicyBuilder = FailoverPolicy.newBuilder();
    if (failover.hasCommonFailoverPolicy()) {
      failoverPolicyBuilder.setCommonFailoverPolicy(
          buildCommonFailoverPolicy(failover.getCommonFailoverPolicy()));
    } else if (failover.hasHybridDeploymentFailoverPolicy()) {
      failoverPolicyBuilder.setExitCodeFailoverPolicy(
          buildExitCodeFailoverPolicy(failover.getHybridDeploymentFailoverPolicy()));
    }
    return failoverPolicyBuilder.build();
  }

  public static SuccessPolicy buildSuccessPolicy(Role role) {
    SuccessPolicy.Builder builder = SuccessPolicy.newBuilder();
    builder.setSuccessPercent(role.getSuccessPercent());
    return builder.build();
  }

  public static List<ResourceRequest> buildResourceRequest(Role role) {
    List<ResourceRequest> resourceRequests = new ArrayList<>();
    resourceRequests.add(
        ResourceRequest.newBuilder()
            .setResourceType(ResourceType.VCORES)
            .setValue(role.getVcores())
            .build()
    );
    resourceRequests.add(
        ResourceRequest.newBuilder()
            .setResourceType(ResourceType.MEMORY_MB)
            .setValue(role.getMemoryMb())
            .build()
    );
    resourceRequests.add(
        ResourceRequest.newBuilder()
            .setResourceType(ResourceType.GPU)
            .setValue(role.getGpuNum())
            .build()
    );
    // default ask for 1 port because tf config need it.
    // consul need 1 port, same as default, so no need to write it here.
    if (role.getUseTfDataService()) {
      resourceRequests.add(
          ResourceRequest.newBuilder()
              .setResourceType(ResourceType.PORT)
              .setValue(1)
              .addValueRanges(
                  ValueRange.newBuilder()
                      .setBegin(5050)
                      .setEnd(5050)
                      .build())
              .build()
      );
    } else {
      resourceRequests.add(
          ResourceRequest.newBuilder()
              .setResourceType(ResourceType.PORT)
              .setValue(role.getPortNum())
              .build()
      );
    }
    return resourceRequests;
  }

  public static InputPolicy buildInputPolicy(Role role) {
    InputPolicy.Builder builder = InputPolicy.newBuilder();
    switch (role.getInputPolicy()) {
      case ENV:
        builder.setEnvInputPolicy(EnvInputPolicy.getDefaultInstance());
        break;
      case STREAMING:
        builder.setStreamingInputPolicy(buildStreamingInputPolicy(role));
        break;
    }
    return builder.build();
  }

  public static CommonFailoverPolicy buildCommonFailoverPolicy(CommonFailover commonFailover) {
    CommonFailoverPolicy.Builder builder = CommonFailoverPolicy.newBuilder();
    switch (commonFailover.getRestartType()) {
      case ON_FAILURE:
        builder.setRestartType(RestartType.ON_FAILURE);
        break;
      case NEVER:
        builder.setRestartType(RestartType.NEVER);
        break;
      case ALWAYS:
        builder.setRestartType(RestartType.ALWAYS);
        break;
    }
    builder.setMaxFailureTimes(commonFailover.getMaxFailureTimes());
    switch (commonFailover.getMaxFailurePolicy()) {
      case FAIL_ATTEMPT:
        builder.setMaxFailurePolicy(MaxFailurePolicy.FAIL_ATTEMPT);
        break;
      case NONE:
        builder.setMaxFailurePolicy(MaxFailurePolicy.NONE);
        break;
      case FAIL_APP:
      default:
        builder.setMaxFailurePolicy(MaxFailurePolicy.FAIL_APP);
        break;
    }
    return builder.build();
  }

  public static ExitCodeFailoverPolicy buildExitCodeFailoverPolicy(
      HybridDeploymentFailover failover) {
    ExitCodeFailoverPolicy.Builder builder = ExitCodeFailoverPolicy.newBuilder();
    builder.setCommonFailoverPolicy(buildCommonFailoverPolicy(failover.getCommonFailover()));
    builder.addRetryableExitCodes(CONTAINER_EXIT_STATUS_KILLED_BY_CONTAINER_EVICTION_MANAGER);
    builder.addRetryableExitCodes(KILLED_BY_HTTP);
    return builder.build();
  }

  public static String buildNodeAttributeExpression(Attribute attribute) {
    List<String> nodeAttributes = new LinkedList<>();
    // Lagrange Lite needs gpu but hasGpu may be false
    // nodeAttributes.add("(has_gpu=" + attribute.getHasGpu() + ")");
    if (!attribute.getGpuName().isEmpty()) {
      nodeAttributes.add("(has_gpu=true)");
      nodeAttributes.add("(gpu_name=" + attribute.getGpuName() + ")");
    }
    if (!attribute.getPod().isEmpty()) {
      nodeAttributes.add("(pod=" + attribute.getPod() + ")");
    }
    for (Map.Entry<String, String> entry : attribute.getPairsMap().entrySet()) {
      nodeAttributes.add("(" + entry.getKey() + "=" + entry.getValue() + ")");
    }
    return String.join("&&", nodeAttributes);
  }

  public static StreamingInputPolicy buildStreamingInputPolicy(Role role) {
    StreamingInputPolicy.Builder builder = StreamingInputPolicy.newBuilder();
    builder.setDataStream(DEFAULT_DATA_STREAM);
    return builder.build();
  }

  public static Data buildData(PrimusConf primusConf) {
    Meta dataMeta = buildMeta(primusConf.getName());
    DataSpec dataSpec = buildDataSpec(primusConf.getInputManager());
    Data data = new Data();
    data.setMeta(dataMeta).setSpec(new DataSpecImpl(dataSpec));
    return data;
  }

  public static DataSpec buildDataSpec(InputManager inputManager) {
    DataSpec.Builder builder = DataSpec.newBuilder();
    if (!inputManager.getConfigCase().equals(ConfigCase.CONFIG_NOT_SET)) {
      builder.putAllDataStreamSpecs(buildDataStreamSpecs(inputManager));
    }
    return builder.build();
  }

  public static Map<String, DataStreamSpec> buildDataStreamSpecs(InputManager inputManager) {
    Map<String, DataStreamSpec> dataStreamSpecs = new HashMap<>();
    dataStreamSpecs.put(DEFAULT_DATA_STREAM, buildDataStreamSpec(inputManager));
    return dataStreamSpecs;
  }

  public static DataStreamSpec buildDataStreamSpec(InputManager inputManager) {
    return DataStreamSpec.newBuilder()
        .addAllDataSourceSpecs(buildDataSourceSpecs(inputManager))
        .build();
  }

  public static List<DataSourceSpec> buildDataSourceSpecs(InputManager inputManager) {
    switch (inputManager.getConfigCase()) {
      case FILE_CONFIG:
        return buildDataSourceSpec(inputManager.getFileConfig());
      case KAFKA_CONFIG:
        return buildDataSourceSpec(inputManager.getKafkaConfig());
      default:
        throw new RuntimeException(
            "Unsupported input manager's config case: " + inputManager.getConfigCase());
    }
  }

  public static List<DataSourceSpec> buildDataSourceSpec(FileConfig config) {
    return buildDataSourceSpecsFromOneTimeInputs(config.getInputsList());
  }

  public static List<DataSourceSpec> buildDataSourceSpec(KafkaConfig kafkaConfig) {
    List<DataSourceSpec> results = new LinkedList<>();
    int sourceId = 0;
    for (Topic topic : kafkaConfig.getTopicsList()) {
      KafkaSourceSpec kafkaSourceSpec = KafkaSourceSpec.newBuilder()
          .setTopic(KafkaSourceSpec.Topic.newBuilder()
              .setTopic(topic.getTopic())
              .setConsumerGroup(topic.getConsumerGroup())
              .putAllConfig(topic.getConfigMap())
              .setKafkaStartUpModeValue(topic.getKafkaStartUpModeValue())
              .setStartUpTimestamp(topic.getStartUpTimestamp())
              .build()
          )
          .setKafkaMessageTypeValue(kafkaConfig.getKafkaMessageTypeValue())
          .build();
      results.add(
          DataSourceSpec.newBuilder()
              .setSourceId(sourceId++)
              .setKafkaSourceSpec(kafkaSourceSpec).build());
    }
    return results;
  }

  public static List<DataSourceSpec> buildDataSourceSpecsFromOneTimeInputs(List<Input> inputs) {
    return IntStream
        .range(0, inputs.size())
        .mapToObj(index -> {
          Input input = inputs.get(index);
          return DataSourceSpec.newBuilder()
              .setSourceId(index)
              .setSource(StringUtils.ensure(input.getName(), String.valueOf(index)))
              .setFileSourceSpec(input.getSpec()) // TODO: Implement FileSourceSpec validator
              .build();
        }).collect(Collectors.toList());
  }

  public static ApiServerConfProto.ApiServerConf buildApiServerConf(ApiServerConf conf) {
    ApiServerConfProto.ApiServerConf.Builder builder = ApiServerConfProto.ApiServerConf
        .newBuilder();
    try {
      builder.mergeFrom(conf.toByteString());
    } catch (InvalidProtocolBufferException ex) {
      LOG.warn("Incompatible ApiServerConf in primus.conf, ignore it.", ex);
    }
    return builder.build();
  }
}
