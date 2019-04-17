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

import com.bytedance.primus.common.model.records.impl.pb.ContainerPBImpl;

/**
 * Trimmed
 */


public abstract class Container implements Comparable<Container> {


  public static Container newInstance(ContainerId containerId, NodeId nodeId,
      String nodeHttpAddress, Resource resource, Priority priority,
      Token containerToken) {
    Container container = new ContainerPBImpl();
    container.setId(containerId);
    container.setNodeId(nodeId);
    container.setNodeHttpAddress(nodeHttpAddress);
    container.setResource(resource);
    container.setPriority(priority);
    container.setContainerToken(containerToken);
    return container;
  }

  /**
   * Get the globally unique identifier for the container.
   *
   * @return globally unique identifier for the container
   */


  public abstract ContainerId getId();


  public abstract void setId(ContainerId id);

  /**
   * Get the identifier of the node on which the container is allocated.
   *
   * @return identifier of the node on which the container is allocated
   */


  public abstract NodeId getNodeId();


  public abstract void setNodeId(NodeId nodeId);

  /**
   * Get the http uri of the node on which the container is allocated.
   *
   * @return http uri of the node on which the container is allocated
   */


  public abstract String getNodeHttpAddress();


  public abstract void setNodeHttpAddress(String nodeHttpAddress);

  /**
   * Get the <code>Resource</code> allocated to the container.
   *
   * @return <code>Resource</code> allocated to the container
   */


  public abstract Resource getResource();


  public abstract void setResource(Resource resource);

  /**
   * Get the <code>Priority</code> at which the <code>Container</code> was allocated.
   *
   * @return <code>Priority</code> at which the <code>Container</code> was
   * allocated
   */


  public abstract Priority getPriority();


  public abstract void setPriority(Priority priority);

  /**
   * Get the <code>ContainerToken</code> for the container.
   * <p><code>ContainerToken</code> is the security token used by the framework
   * to verify authenticity of any <code>Container</code>.</p>
   *
   * <p>The <code>ResourceManager</code>, on container allocation provides a
   * secure token which is verified by the <code>NodeManager</code> on container launch.</p>
   *
   * <p>Applications do not need to care about <code>ContainerToken</code>, they
   * are transparently handled by the framework - the allocated
   * <code>Container</code> includes the <code>ContainerToken</code>.</p>
   *
   * @return <code>ContainerToken</code> for the container
   * see ApplicationMasterProtocol#allocate(com.bytedance.primus.common.api.protocolrecords.AllocateRequest)
   * see ContainerManagementProtocol#startContainers(com.bytedance.primus.common.api.protocolrecords.StartContainersRequest)
   */


  public abstract Token getContainerToken();


  public abstract void setContainerToken(Token containerToken);


  public abstract boolean getIsGuaranteed();


  public abstract void setIsGuaranteed(boolean isGuaranteed);
}
