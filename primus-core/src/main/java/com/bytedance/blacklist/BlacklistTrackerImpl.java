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

package com.bytedance.blacklist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class BlacklistTrackerImpl implements BlacklistTracker {

  private static final Logger LOG = LoggerFactory.getLogger(BlacklistTrackerImpl.class);

  private int maxFailedTaskPerContainer;
  private int maxFailedContainerPerNode;
  private int maxBlacklistContainer;
  private int maxBlacklistNode;
  private long blacklistTimeoutMillis;
  private boolean enabled;
  private Map<String, FailureInfo> containerFailureInfoMap = new ConcurrentHashMap<>();
  private Map<String, FailureInfo> nodeFailureInfoMap = new ConcurrentHashMap<>();
  private Map<String, Long> containerBlacklist = new ConcurrentHashMap<>();
  private Map<String, Long> nodeBlacklist = new ConcurrentHashMap<>();
  private Map<String, String> containerNodeMap = new ConcurrentHashMap<>();

  public BlacklistTrackerImpl(
      int maxFailedTaskPerContainer,
      int maxFailedContainerPerNode,
      long blacklistTimeoutMillis,
      int maxBlacklistContainer,
      int maxBlacklistNode) {
    this(
        maxFailedTaskPerContainer,
        maxFailedContainerPerNode,
        blacklistTimeoutMillis,
        maxBlacklistContainer,
        maxBlacklistNode,
        false);
  }

  public BlacklistTrackerImpl(
      int maxFailedTaskPerContainer,
      int maxFailedContainerPerNode,
      long blacklistTimeoutMillis,
      int maxBlacklistContainer,
      int maxBlacklistNode,
      boolean enabled) {
    this.maxFailedTaskPerContainer = maxFailedTaskPerContainer;
    this.maxFailedContainerPerNode = maxFailedContainerPerNode;
    this.blacklistTimeoutMillis = blacklistTimeoutMillis;
    this.maxBlacklistContainer = maxBlacklistContainer;
    this.maxBlacklistNode = maxBlacklistNode;
    this.enabled = enabled;
    if (enabled) {
      LOG.info("Blacklist is enabled");
    } else {
      LOG.info("Blacklist is not enabled");
    }
  }

  @Override
  public synchronized void addTaskFailure(
      String taskId,
      String containerId,
      String nodeId,
      long taskFailureTimeMillis) {
    if (!enabled) {
      return;
    }
    if (!containerNodeMap.containsKey(containerId)) {
      containerNodeMap.put(containerId, nodeId);
    }
    FailureInfo containerFailureInfo =
        containerFailureInfoMap.getOrDefault(containerId,
            new FailureInfo(containerId,
                containerBlacklist,
                maxFailedTaskPerContainer,
                maxBlacklistContainer));
    LOG.info("Add task failure. "
        + "taskId[" + taskId + "], "
        + "containerId[" + containerId + "], "
        + "nodeId[" + nodeId + "], "
        + "taskFailureTimeMillis[" + taskFailureTimeMillis + "].");
    String blacklistedContainerId =
        containerFailureInfo.addFailure(
            taskId, taskFailureTimeMillis + blacklistTimeoutMillis);
    if (blacklistedContainerId != null) {
      addContainerFailure(blacklistedContainerId, nodeId, System.currentTimeMillis());
    }
    containerFailureInfoMap.put(containerId, containerFailureInfo);
  }

  @Override
  public synchronized void addContainerFailure(
      String containerId,
      String nodeId,
      long containerFailureTimeMillis) {
    if (!enabled) {
      return;
    }
    if (!containerNodeMap.containsKey(containerId)) {
      containerNodeMap.put(containerId, nodeId);
    }
    FailureInfo nodeFailureInfo =
        nodeFailureInfoMap.getOrDefault(nodeId,
            new FailureInfo(nodeId,
                nodeBlacklist,
                maxFailedContainerPerNode,
                maxBlacklistNode));
    LOG.info("Add container failure."
        + "containerId[" + containerId + "], "
        + "nodeId[" + nodeId + "], "
        + "containerFailureTimeMillis[" + containerFailureTimeMillis + "]");
    nodeFailureInfo.addFailure(containerId,
        containerFailureTimeMillis + blacklistTimeoutMillis);
    nodeFailureInfoMap.put(nodeId, nodeFailureInfo);
  }

  @Override
  public synchronized boolean isContainerBlacklisted(String containerId) {
    long now = System.currentTimeMillis();
    for (Map.Entry<String, Long> entry : containerBlacklist.entrySet()) {
      if (entry.getValue() < now) {
        containerBlacklist.remove(entry.getKey());
        LOG.info("Container[" + entry.getKey() + "] is removed from container blacklist.");
      } else {
        break;
      }
    }
    return containerBlacklist.containsKey(containerId);
  }

  @Override
  public synchronized boolean isNodeBlacklisted(String nodeId) {
    releaseExpiredNode();
    return nodeBlacklist.containsKey(nodeId);
  }

  @Override
  public synchronized Map<String, Long> getNodeBlacklist() {
    releaseExpiredNode();
    return nodeBlacklist;
  }

  private void releaseExpiredNode() {
    long now = System.currentTimeMillis();
    for (Map.Entry<String, Long> entry : nodeBlacklist.entrySet()) {
      if (entry.getValue() < now) {
        nodeBlacklist.remove(entry.getKey());
        LOG.info("Node[" + entry.getKey() + "] is removed from node blacklist.");
      } else {
        break;
      }
    }
  }

  @Override
  public void addNodeBlackList(Map<String, Long> blackList) {
    nodeBlacklist.putAll(blackList);
  }

  @Override
  public void removeNodeBlackList(Map<String, Long> blackList) {
    for (String key : blackList.keySet()) {
      nodeBlacklist.remove(key);
    }
  }

  class FailureInfo {

    private Map<String, Long> idExpiryTimeMap = new ConcurrentHashMap<>();
    private String parentId;
    private Map<String, Long> blacklist;
    private int maxFailureNum;
    private int maxBlacklistedParentNum;

    public FailureInfo(
        String parentId,
        Map<String, Long> blacklist,
        int maxFailureNum,
        int maxBlacklistedParentNum) {
      this.parentId = parentId;
      this.blacklist = blacklist;
      this.maxFailureNum = maxFailureNum;
      this.maxBlacklistedParentNum = maxBlacklistedParentNum;
    }

    public String addFailure(String id, long expiryTime) {
      String ret = null;
      long oldExpiryTime = idExpiryTimeMap.getOrDefault(id, 0L);
      if (oldExpiryTime < expiryTime) {
        idExpiryTimeMap.put(id, expiryTime);
      }

      long now = System.currentTimeMillis();

      for (Map.Entry<String, Long> entry : idExpiryTimeMap.entrySet()) {
        if (entry.getValue() < now) {
          idExpiryTimeMap.remove(entry.getKey());
        }
      }

      if (idExpiryTimeMap.size() > maxFailureNum) {
        long parentExpiryTime = now + blacklistTimeoutMillis;
        if (blacklist.containsKey(parentId)) {
          long parentOldExpiryTime = blacklist.get(parentId);
          if (parentOldExpiryTime < parentExpiryTime) {
            blacklist.remove(parentId);
            blacklist.put(parentId, parentExpiryTime);
            LOG.info("update [" + parentId + "] in blacklist, expiryTime[" + expiryTime + "]");
          }
        } else if (blacklist.size() < maxBlacklistedParentNum) {
          blacklist.put(parentId, parentExpiryTime);
          LOG.info("Add [" + parentId + "] to blacklist, expiryTime[" + expiryTime + "]");
          ret = parentId;
        } else {
          LOG.warn("Blacklist num has exceeded max num[" + maxBlacklistedParentNum + "]");
        }
      }
      return ret;
    }

    public String getParentId() {
      return parentId;
    }
  }
}
