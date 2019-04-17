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

package com.bytedance.primus.runtime.yarncommunity.utils;

import com.bytedance.primus.common.exceptions.PrimusIllegalArgumentException;
import com.bytedance.primus.common.model.protocolrecords.ResourceTypes;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.ApplicationId;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.common.model.records.NodeId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.model.records.Resource;
import com.bytedance.primus.common.model.records.ResourceInformation;
import com.bytedance.primus.common.model.records.Token;
import com.bytedance.primus.common.model.records.impl.pb.ResourcePBImpl;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;

// TODO: Create interfaces and wrap up the underlying runtime depending models, so that we don't
//  have to worry much on unmapped fields.
public abstract class YarnConvertor {

  public static Container
  toPrimusContainer(org.apache.hadoop.yarn.api.records.Container container) {
    Container ret = Container.newInstance(
        toPrimusContainerId(container.getId()),
        toPrimusNodeId(container.getNodeId()),
        container.getNodeHttpAddress(),
        toPrimusResource(container.getResource()),
        toPrimusPriority(container.getPriority()),
        toPrimusContainerToken(container.getContainerToken())
    );

    ret.setIsGuaranteed(container.getExecutionType() == ExecutionType.GUARANTEED);
    return ret;
  }

  public static org.apache.hadoop.yarn.api.records.Container
  toYarnContainer(Container container) {
    org.apache.hadoop.yarn.api.records.Container ret =
        org.apache.hadoop.yarn.api.records.Container.newInstance(
            toYarnContainerId(container.getId()),
            toYarnNodeId(container.getNodeId()),
            container.getNodeHttpAddress(),
            toYarnResource(container.getResource()),
            toYarnPriority(container.getPriority()),
            toYarnContainerToken(container.getContainerToken())
        );

    ret.setExecutionType(container.getIsGuaranteed()
        ? ExecutionType.GUARANTEED
        : ExecutionType.OPPORTUNISTIC);
    return ret;
  }

  private static ContainerId
  toPrimusContainerId(org.apache.hadoop.yarn.api.records.ContainerId containerId) {
    return ContainerId.newContainerId(
        toPrimusApplicationAttemptId(containerId.getApplicationAttemptId()),
        containerId.getContainerId()
    );
  }

  private static org.apache.hadoop.yarn.api.records.ContainerId
  toYarnContainerId(ContainerId containerId) {
    return org.apache.hadoop.yarn.api.records.ContainerId.newContainerId(
        toYarnApplicationAttemptId(containerId.getApplicationAttemptId()),
        containerId.getContainerId()
    );
  }

  private static ApplicationAttemptId
  toPrimusApplicationAttemptId(org.apache.hadoop.yarn.api.records.ApplicationAttemptId attemptId) {
    return ApplicationAttemptId.newInstance(
        toPrimusApplicationId(attemptId.getApplicationId()),
        attemptId.getAttemptId()
    );
  }

  private static org.apache.hadoop.yarn.api.records.ApplicationAttemptId
  toYarnApplicationAttemptId(ApplicationAttemptId attemptId) {
    return org.apache.hadoop.yarn.api.records.ApplicationAttemptId.newInstance(
        toYarnApplicationId(attemptId.getApplicationId()),
        attemptId.getAttemptId()
    );
  }

  private static ApplicationId
  toPrimusApplicationId(org.apache.hadoop.yarn.api.records.ApplicationId applicationId) {
    return ApplicationId.newInstance(
        applicationId.getClusterTimestamp(),
        applicationId.getId()
    );
  }

  private static org.apache.hadoop.yarn.api.records.ApplicationId
  toYarnApplicationId(ApplicationId applicationId) {
    return org.apache.hadoop.yarn.api.records.ApplicationId.newInstance(
        applicationId.getClusterTimestamp(),
        applicationId.getId()
    );
  }

  private static NodeId
  toPrimusNodeId(org.apache.hadoop.yarn.api.records.NodeId nodeId) {
    return NodeId.newInstance(
        nodeId.getHost(),
        nodeId.getPort()
    );
  }

  private static org.apache.hadoop.yarn.api.records.NodeId
  toYarnNodeId(NodeId nodeId) {
    return org.apache.hadoop.yarn.api.records.NodeId.newInstance(
        nodeId.getHost(),
        nodeId.getPort()
    );
  }

  private static Resource
  toPrimusResource(org.apache.hadoop.yarn.api.records.Resource resource) {
    return new ResourcePBImpl(
        0 /* Yarn convention for memory */,
        1 /* Yarn convention for vcores */,
        toPrimusResourceInformation(resource.getResources())
    );
  }

  private static org.apache.hadoop.yarn.api.records.Resource
  toYarnResource(Resource resource) {
    return new org.apache.hadoop.yarn.api.records.impl.LightWeightResource(
        resource.getMemory(),
        resource.getVirtualCores(),
        toYarnResourceInformation(resource.getResources())
    );
  }

  private static ResourceInformation[]
  toPrimusResourceInformation(org.apache.hadoop.yarn.api.records.ResourceInformation[] infos) {
    return Arrays.stream(infos).map(info ->
        ResourceInformation.newInstance(
            info.getName(),
            info.getUnits(),
            info.getValue(),
            toPrimusResourceType(info.getResourceType()),
            info.getMinimumAllocation(),
            info.getMaximumAllocation()
        )).toArray(ResourceInformation[]::new);
  }

  private static org.apache.hadoop.yarn.api.records.ResourceInformation[]
  toYarnResourceInformation(ResourceInformation[] infos) {
    return Arrays.stream(infos).map(info ->
        org.apache.hadoop.yarn.api.records.ResourceInformation.newInstance(
            info.getName(),
            info.getUnits(),
            info.getValue(),
            toYarnResourceType(info.getResourceType()),
            info.getMinimumAllocation(),
            info.getMaximumAllocation()
        )).toArray(org.apache.hadoop.yarn.api.records.ResourceInformation[]::new);
  }

  private static ResourceTypes
  toPrimusResourceType(org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes resourceType) {
    switch (resourceType) {
      case COUNTABLE:
        return ResourceTypes.COUNTABLE;
    }
    throw new PrimusIllegalArgumentException("Unknown ResourceType: " + resourceType);
  }

  private static org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes
  toYarnResourceType(ResourceTypes resourceType) {
    switch (resourceType) {
      case COUNTABLE:
        return org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes.COUNTABLE;
    }
    throw new PrimusIllegalArgumentException("Unknown ResourceType: " + resourceType);
  }

  private static Priority
  toPrimusPriority(org.apache.hadoop.yarn.api.records.Priority priority) {
    return Priority.newInstance(priority.getPriority());
  }

  private static org.apache.hadoop.yarn.api.records.Priority
  toYarnPriority(Priority priority) {
    return org.apache.hadoop.yarn.api.records.Priority.newInstance(priority.getPriority());
  }

  private static Token
  toPrimusContainerToken(org.apache.hadoop.yarn.api.records.Token token) {
    return Token.newInstance(
        token.getIdentifier().array(),
        token.getKind(),
        token.getPassword().array(),
        token.getService()
    );
  }

  private static org.apache.hadoop.yarn.api.records.Token
  toYarnContainerToken(Token token) {
    return org.apache.hadoop.yarn.api.records.Token.newInstance(
        token.getIdentifier().array(),
        token.getKind(),
        token.getPassword().array(),
        token.getService()
    );
  }

  public static org.apache.hadoop.yarn.api.records.FinalApplicationStatus
  toYarnFinalApplicationStatus(FinalApplicationStatus primus) {
    switch (primus) {
      case SUCCEEDED:
        return org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;
      case FAILED:
        return org.apache.hadoop.yarn.api.records.FinalApplicationStatus.FAILED;
      case KILLED:
        return org.apache.hadoop.yarn.api.records.FinalApplicationStatus.KILLED;
      default: // UNDEFINED
        return org.apache.hadoop.yarn.api.records.FinalApplicationStatus.UNDEFINED;
    }
  }

  public static Map<String, String> toMetricToMap(ContainerStatus containerStatus) {
    return new HashMap<String, String>() {{
      // TODO: Fulfill these
    }};
  }
}
