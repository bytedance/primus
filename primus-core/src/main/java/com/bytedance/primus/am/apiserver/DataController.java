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
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.TaskManagerState;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.apis.watch.ResourceEventHandler;
import com.bytedance.primus.apiserver.client.apis.watch.Watch;
import com.bytedance.primus.apiserver.client.models.Data;
import com.bytedance.primus.apiserver.proto.DataProto.DataStreamStatus.DataStreamState;
import com.bytedance.primus.apiserver.records.DataSourceStatus;
import com.bytedance.primus.apiserver.records.DataStatus;
import com.bytedance.primus.apiserver.records.DataStreamStatus;
import com.bytedance.primus.apiserver.records.impl.DataSourceStatusImpl;
import com.bytedance.primus.apiserver.records.impl.DataStatusImpl;
import com.bytedance.primus.apiserver.records.impl.DataStreamStatusImpl;
import com.bytedance.primus.common.service.AbstractService;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataController extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(DataController.class);
  private static final int RECONCILE_INTERVAL = 20 * 1000;

  private AMContext context;
  private CoreApi coreApi;
  private Watch<Data> dataWatch;

  private Thread reconcileThread;
  private volatile boolean isStopped;
  private volatile String dataName;

  public DataController(AMContext context) {
    super(DataController.class.getName());
    this.context = context;
    coreApi = context.getCoreApi();
    isStopped = false;
    dataName = null;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    dataWatch = coreApi.createWatchList(Data.class, new DataEventHandler());
    reconcileThread = new DataReconcileThread();
    reconcileThread.setDaemon(true);
    reconcileThread.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;
    dataWatch.cancel();
    reconcileThread.interrupt();
  }

  public Data getData() {
    Data data = null;
    if (dataName != null) {
      try {
        data = coreApi.getData(dataName);
      } catch (Exception e) {
        LOG.warn("Failed to get data from api server", e);
      }
    }
    return data;
  }

  private void updateDataToApiServer(Data data) {
    try {
      Data newData = coreApi.replaceData(data, data.getMeta().getVersion());
      LOG.info("Data replaced\n{}", newData);
    } catch (Exception e) {
      LOG.warn("Failed to replace data " + data.getMeta().getName(), e);
    }
  }

  private void updateDataStreamStatuses(Map<String, DataStreamStatus> dataStreamStatusMap) {
    Map<String, TaskManager> taskManagerMap = context.getDataStreamManager().getTaskManagerMap();
    for (Map.Entry<String, TaskManager> entry : taskManagerMap.entrySet()) {
      DataStreamStatus dataStreamStatus = new DataStreamStatusImpl();
      dataStreamStatus.setState(getDataStreamState(entry.getValue().getState()));
      dataStreamStatus.setProgress(entry.getValue().getProgress());
      dataStreamStatus.setDataSourceStatuses(
          getDataSourceStatuses(entry.getValue().getDataSourceReports()));
      dataStreamStatusMap.put(entry.getKey(), dataStreamStatus);
    }
  }

  private List<DataSourceStatus> getDataSourceStatuses(Map<Integer, String> dataSourceBatchKeyMap) {
    return dataSourceBatchKeyMap.entrySet().stream().map(e -> {
      DataSourceStatus dataSourceStatus = new DataSourceStatusImpl();
      dataSourceStatus.setSourceId(e.getKey());
      dataSourceStatus.setReport(e.getValue());
      return dataSourceStatus;
    }).collect(Collectors.toList());
  }

  private DataStreamState getDataStreamState(TaskManagerState taskManagerState) {
    return DataStreamState.valueOf(taskManagerState.name());
  }

  class DataReconcileThread extends Thread {

    public DataReconcileThread() {
      super(DataReconcileThread.class.getName());
    }

    @Override
    public void run() {
      while (!isStopped) {
        try {
          Thread.sleep(RECONCILE_INTERVAL);
        } catch (InterruptedException e) {
          // ignore
        }
        if (dataName == null) {
          continue;
        }
        try {
          Data data = coreApi.getData(dataName);
          DataStatus newDataStatus = new DataStatusImpl(data.getStatus().getProto());
          updateDataStreamStatuses(newDataStatus.getDataStreamStatuses());
          // Only update the difference to api server
          if (!newDataStatus.equals(data.getStatus())) {
            data.setStatus(newDataStatus);
            updateDataToApiServer(data);
          }
        } catch (Exception e) {
          LOG.warn("Failed to reconcile data", e);
        }
      }
    }
  }

  class DataEventHandler implements ResourceEventHandler<Data> {

    @Override
    public void onAdd(Data data) {
      LOG.info("Data added\n{}", data);
      DataController.this.dataName = data.getMeta().getName();
      context.emitDataInputCreatedEvent(data);
    }

    @Override
    public void onUpdate(Data oldData, Data newData) {
      LOG.info("Data updated\n{}\n{}", oldData, newData);
      if (oldData.getSpec().equals(newData.getSpec())) {
        LOG.info("Data spec not changed");
      } else {
        LOG.info("Data spec changed");
        context.emitDataInputUpdatedEvent(newData);
      }
    }

    @Override
    public void onDelete(Data data) {
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error(DataEventHandler.class.getName() + " catches error", throwable);
    }
  }
}