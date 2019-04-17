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

package com.bytedance.primus.runtime.kubernetesnative.am.operator;

import com.bytedance.primus.common.service.AbstractService;
import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesAMContext;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.OperatorAPIServer;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.model.APIServerEndPoint;
import org.apache.commons.configuration.Configuration;

public class OperatorStateAPIService extends AbstractService {

  private KubernetesAMContext context;
  private OperatorAPIServer apiServer;

  public OperatorStateAPIService(KubernetesAMContext context) {
    super(OperatorStateAPIService.class.getName());
    this.context = context;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    String host = context.getApiServerHost();
    int port = context.getApiServerPort();
    APIServerEndPoint apiServerEndPoint = new APIServerEndPoint(host, port);
    apiServer = new OperatorAPIServer(apiServerEndPoint, context.getAppName());
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    apiServer.start();
  }

  @Override
  protected void serviceStop() throws Exception {

  }
}
