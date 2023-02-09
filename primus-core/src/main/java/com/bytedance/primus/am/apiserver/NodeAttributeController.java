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

package com.bytedance.primus.am.apiserver;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.apis.watch.ResourceEventHandler;
import com.bytedance.primus.apiserver.client.apis.watch.Watch;
import com.bytedance.primus.apiserver.client.models.NodeAttribute;
import com.bytedance.primus.apiserver.proto.ResourceProto;
import com.bytedance.primus.common.service.AbstractService;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeAttributeController extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(NodeAttributeController.class);
  private static final int RECONCILE_INTERVAL = 20 * 1000;

  private AMContext context;
  private CoreApi coreApi;
  private Watch<NodeAttribute> dataWatch;

  private volatile boolean isStopped;

  public NodeAttributeController(AMContext context) {
    super(NodeAttributeController.class.getName());
    this.context = context;
    coreApi = context.getCoreApi();
    isStopped = false;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    dataWatch =
        coreApi.createWatchList(
            NodeAttribute.class, new NodeAttributeController.NodeAttributeEventHandler());
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;
    dataWatch.cancel();
  }

  class NodeAttributeEventHandler implements ResourceEventHandler<NodeAttribute> {
    @Override
    public void onAdd(NodeAttribute nodeAttribute) {
      LOG.info("NodeAttribute added\n{}", nodeAttribute.toString());

      Map<String, Long> blackListMap = Maps.newHashMap(nodeAttribute.getSpec().getProto().getBlackListMap());
      long creationTime = nodeAttribute.getMeta().getCreationTime();
      for (String node : blackListMap.keySet()) {
        Long expiredTime = blackListMap.get(node);
        blackListMap.put(node, expiredTime + creationTime);
      }
      context.getBlacklistTracker().ifPresent(b -> b.addNodeBlackList(blackListMap));
    }

    @Override
    public void onUpdate(NodeAttribute oldNodeAttribute, NodeAttribute newNodeAttribute) {
      LOG.info("NodeAttribute updated\n{}", newNodeAttribute.toString());

      Map<String, Long> oldBlackList =
          Maps.newHashMap(oldNodeAttribute.getSpec().getProto().getBlackListMap());
      Map<String, Long> newBlackList =
          Maps.newHashMap(newNodeAttribute.getSpec().getProto().getBlackListMap());
      if (oldBlackList.equals(newBlackList)) {
        LOG.info("NodeAttribute spec not changed");
        return;
      }
      LOG.info("NodeAttribute spec changed");
      Map<String, Long> remainBlackList =
          oldBlackList.entrySet().stream()
              .filter(
                  entry ->
                      newBlackList.containsKey(entry.getKey())
                          && newBlackList.get(entry.getKey()).equals(entry.getValue()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      remainBlackList.forEach(newBlackList::remove);
      remainBlackList.forEach(oldBlackList::remove);
      newBlackList.forEach(
          (node, time) ->
              LOG.info("add node [" + node + "] to blacklist ,expired after " + time + " ms"));
      oldBlackList.forEach((node, time) -> LOG.info("remove node [" + node + "] from blacklist"));
      long creationTime = newNodeAttribute.getMeta().getCreationTime();
      for (String node : newBlackList.keySet()) {
        Long expiredTime = newBlackList.get(node);
        newBlackList.put(node, expiredTime + creationTime);
      }
      context.getBlacklistTracker().ifPresent(b -> b.addNodeBlackList(newBlackList));
      context.getBlacklistTracker().ifPresent(b -> b.removeNodeBlackList(oldBlackList));
    }

    @Override
    public void onDelete(NodeAttribute nodeAttribute) {
      LOG.info("NodeAttribute deleted\n{}", nodeAttribute.toString());
      ResourceProto.NodeAttributeSpec spec = nodeAttribute.getSpec().getProto();
      context
          .getBlacklistTracker()
          .ifPresent(b -> b.removeNodeBlackList(spec.getBlackListMap()));
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error(
          NodeAttributeController.NodeAttributeEventHandler.class.getName() + " catches error",
          throwable);
    }
  }
}
