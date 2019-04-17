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

import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.apis.watch.ResourceEventHandler;
import com.bytedance.primus.apiserver.client.apis.watch.Watch;
import com.bytedance.primus.apiserver.client.models.DataSavepoint;
import com.bytedance.primus.apiserver.proto.DataProto.DataSavepointStatus.DataSavepointState;
import com.bytedance.primus.common.service.AbstractService;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSavepointController extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(DataSavepointController.class);

  private CoreApi coreApi;
  private DataStreamManager dataStreamManager;
  private Watch<DataSavepoint> dataSavepointWatch;

  public DataSavepointController(CoreApi coreApi, DataStreamManager dataStreamManager) {
    super(DataSavepointController.class.getName());
    this.coreApi = coreApi;
    this.dataStreamManager = dataStreamManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    dataSavepointWatch = coreApi
        .createWatchList(DataSavepoint.class, new DataSavepointEventHandler());
  }

  @Override
  protected void serviceStop() throws Exception {
    dataSavepointWatch.cancel();
  }

  class DataSavepointEventHandler implements ResourceEventHandler<DataSavepoint> {

    @Override
    public void onAdd(DataSavepoint dataSavepoint) {
      LOG.info("DataSavepoint added\n{}", dataSavepoint);
      new DataSavepointThread(dataSavepoint).start();
    }

    @Override
    public void onUpdate(DataSavepoint oldDataSavepoint, DataSavepoint newDataSavepoint) {
      LOG.info("DataSavepoint updated\n{}\n{}", oldDataSavepoint, newDataSavepoint);
      if (oldDataSavepoint.getSpec().equals(newDataSavepoint.getSpec())) {
        LOG.info("DataSavepoint spec not changed");
      } else {
        LOG.info("DataSavepoint spec changed");
        new DataSavepointThread(newDataSavepoint).start();
      }
    }

    @Override
    public void onDelete(DataSavepoint dataSavepoint) {
      LOG.info("DataSavepoint deleted\n{}", dataSavepoint);
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error(DataSavepointEventHandler.class.getName() + " catches error", throwable);
    }
  }

  class DataSavepointThread extends Thread {

    DataSavepoint dataSavepoint;

    public DataSavepointThread(DataSavepoint dataSavepoint) {
      super(DataSavepointThread.class.getName());
      this.dataSavepoint = dataSavepoint;
      setDaemon(true);
    }

    @Override
    public void run() {
      String savepointDir = dataSavepoint.getSpec().getSavepointDir();
      dataSavepoint.getStatus().setState(DataSavepointState.RUNNING);
      updateDataSavepointToApiServer(dataSavepoint);
      LOG.info("Savepoint running {}", dataSavepoint);
      if (dataStreamManager.makeSavepoint(savepointDir)) {
        dataSavepoint.getStatus().setState(DataSavepointState.SUCCEEDED);
        LOG.info("Savepoint succeed {}", dataSavepoint);
      } else {
        dataSavepoint.getStatus().setState(DataSavepointState.FAILED);
        LOG.info("Savepoint failed {}", dataSavepoint);
      }
      updateDataSavepointToApiServer(dataSavepoint);
    }

    private void updateDataSavepointToApiServer(DataSavepoint dataSavepoint) {
      try {
        DataSavepoint dcp = coreApi.getDataSavepoint(dataSavepoint.getMeta().getName());
        DataSavepoint newDataSavepoint = coreApi
            .replaceDataSavepoint(dataSavepoint, dcp.getMeta().getVersion());
        LOG.info("DataSavepoint replaced\n{}", newDataSavepoint);
      } catch (Exception e) {
        LOG.warn("Failed to replace dataSavepoint " + dataSavepoint.getMeta().getName(), e);
      }
    }
  }
}
