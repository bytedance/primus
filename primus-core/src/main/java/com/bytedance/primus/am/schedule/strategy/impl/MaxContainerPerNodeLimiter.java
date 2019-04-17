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

package com.bytedance.primus.am.schedule.strategy.impl;

import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.NodeId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxContainerPerNodeLimiter {

  private static final Logger log = LoggerFactory.getLogger(MaxContainerPerNodeLimiter.class);

  private int maxContainerPerNode;

  public MaxContainerPerNodeLimiter(int maxContainerPerNode) {
    this.maxContainerPerNode = maxContainerPerNode;
    log.info("Create MaxContainerPerNodeLimiter: {}.", maxContainerPerNode);
  }

  private Map<NodeId, Set<ContainerId>> containerCounterMap = new HashMap<>();

  public void addContainer(Container container) {
    containerCounterMap.computeIfAbsent(container.getNodeId(), key -> new HashSet<ContainerId>());
    containerCounterMap.get(container.getNodeId()).add(container.getId());
  }

  public void removeContainer(Container container) {
    containerCounterMap.computeIfAbsent(container.getNodeId(), key -> new HashSet<ContainerId>());
    containerCounterMap.get(container.getNodeId()).remove(container.getId());
  }

  public boolean isNodeFull(NodeId priority) {
    containerCounterMap.putIfAbsent(priority, new HashSet<ContainerId>());
    if (maxContainerPerNode == 0) {
      return false;
    }
    return containerCounterMap.get(priority).size() >= maxContainerPerNode;
  }

}
