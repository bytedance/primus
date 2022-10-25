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

package com.bytedance.primus.executor.worker.launchplugin;

import com.bytedance.primus.api.records.ClusterSpec;
import com.bytedance.primus.api.records.Endpoint;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.api.records.ExecutorSpecs;
import com.bytedance.primus.apiserver.client.models.Job;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.worker.WorkerContext;
import java.util.Map;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LaunchPlugin(value = "TfConfigPlugin", frameworkDefault = true)
public class TfConfigPlugin implements ChildLaunchPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(TfConfigPlugin.class);
  private static final String TF_CONFIG_ENV_KEY = "TF_CONFIG";

  private ExecutorContext executorContext;
  private WorkerContext workerContext;

  public TfConfigPlugin(ExecutorContext executorContext, WorkerContext workerContext) {
    this.executorContext = executorContext;
    this.workerContext = workerContext;
  }

  @Override
  public void init() throws Exception {
  }

  @Override
  public void preStart() throws Exception {
    Job job = executorContext.getPrimusExecutorConf().getCoreApi().listJobs().get(0);
    int totalExecutorNum = 0;
    for (RoleSpec roleSpec : job.getSpec().getRoleSpecs().values()) {
      totalExecutorNum += roleSpec.getReplicas();
    }
    if (totalExecutorNum != 1) {
      // Lagrange lite does not need TF_CONFIG when executor number == 1
      JSONObject taskinfo = new JSONObject();
      taskinfo.put("type", executorContext.getExecutorId().getRoleName());
      taskinfo.put("index", executorContext.getExecutorId().getIndex());
      JSONObject tfConfig = new JSONObject();
      tfConfig.put("cluster", buildClusterSpecStr(workerContext.getClusterSpec()));
      tfConfig.put("task", taskinfo);
      workerContext.getEnvironment().put(TF_CONFIG_ENV_KEY, tfConfig.toString());
    }
  }

  @Override
  public void postStart() throws Exception {
  }

  @Override
  public void preStop() throws Exception {
  }

  @Override
  public void postStop() throws Exception {
  }

  private JSONObject buildClusterSpecStr(ClusterSpec clusterSpec) {
    JSONObject cluster = new JSONObject();
    try {
      for (Map.Entry<String, ExecutorSpecs> entry : clusterSpec.getExecutorSpecs().entrySet()) {
        JSONArray executorSpecArray = new JSONArray();
        for (ExecutorSpec executorSpec : entry.getValue().getExecutorSpecs()) {
          if (executorSpec.getEndpoints().isEmpty()) {
            executorSpecArray.put("");
          } else {
            Endpoint endpoint = executorSpec.getEndpoints().get(0);
            String executorSpecStr = endpoint.getHostname() + ":" + endpoint.getPort();
            executorSpecArray.put(executorSpecStr);
          }
        }
        cluster.put(entry.getKey(), executorSpecArray);
      }
    } catch (JSONException e) {
      LOG.error("build cluster spec failed", e);
    }
    return cluster;
  }
}
