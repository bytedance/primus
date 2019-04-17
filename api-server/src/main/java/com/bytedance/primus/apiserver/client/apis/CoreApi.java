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

package com.bytedance.primus.apiserver.client.apis;

import com.bytedance.primus.apiserver.client.Client;
import com.bytedance.primus.apiserver.client.apis.watch.ResourceEventHandler;
import com.bytedance.primus.apiserver.client.apis.watch.Watch;
import com.bytedance.primus.apiserver.client.models.ApiType;
import com.bytedance.primus.apiserver.client.models.Data;
import com.bytedance.primus.apiserver.client.models.DataSavepoint;
import com.bytedance.primus.apiserver.client.models.DataSource;
import com.bytedance.primus.apiserver.client.models.DataStream;
import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.client.models.Job;
import com.bytedance.primus.apiserver.client.models.NodeAttribute;
import com.bytedance.primus.apiserver.client.models.Role;
import com.bytedance.primus.apiserver.client.models.Worker;
import com.bytedance.primus.apiserver.records.Resource;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.apiserver.service.filter.FilterKey;
import com.bytedance.primus.apiserver.utils.ReflectionUtils;
import com.google.common.collect.Maps;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoreApi {

  private static final Logger LOG = LoggerFactory.getLogger(CoreApi.class);

  private Client client;

  public CoreApi(Client client) {
    this.client = client;
  }

  public <T extends ApiType> T create(Class<T> tClass, T obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return (T) ReflectionUtils.newInstance(tClass).fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void delete(String resourceKind, String resourceName) throws ApiServerException {
    try {
      client.delete(resourceKind, resourceName);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteList(String resourceKind) throws ApiServerException {
    try {
      client.deleteList(resourceKind);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public <T extends ApiType> T replace(Class<T> tClass, T obj, long resourceVersion)
      throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return (T) ReflectionUtils.newInstance(tClass).fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public <T extends ApiType> T get(Class<T> tClass, String resourceKind, String resourceName)
      throws ApiServerException {
    try {
      Resource resource = client.get(resourceKind, resourceName, null);
      return (T) ReflectionUtils.newInstance(tClass).fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public <T extends ApiType> List<T> list(Class<T> tClass, String resourceKind)
      throws ApiServerException {
    try {
      List<Resource> resources = client.list(resourceKind, null);
      List<T> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add((T) ReflectionUtils.newInstance(tClass).fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public <T extends ApiType> Watch<T> createWatch(
      Class<T> tClass, String resourceName, ResourceEventHandler<T> resourceEventHandler)
      throws ApiServerException {
    try {
      return new Watch<>(client, tClass, resourceName, resourceEventHandler);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public <T extends ApiType> Watch<T> createWatchList(
      Class<T> tClass, ResourceEventHandler<T> resourceEventHandler) throws ApiServerException {
    try {
      return new Watch<>(client, tClass, resourceEventHandler);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Job createJob(Job obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return new Job().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteJob(String name) throws ApiServerException {
    try {
      client.delete(Job.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteJobs() throws ApiServerException {
    try {
      client.deleteList(Job.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Job replaceJob(Job obj, long resourceVersion) throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return new Job().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Job getJob(String name) throws ApiServerException {
    try {
      Resource resource = client.get(Job.KIND, name, null);
      return new Job().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Job> listJobs() throws ApiServerException {
    try {
      List<Resource> resources = client.list(Job.KIND, null);
      List<Job> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new Job().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Role createRole(Role obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return new Role().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteRole(String name) throws ApiServerException {
    try {
      client.delete(Role.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteRoles() throws ApiServerException {
    try {
      client.deleteList(Role.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Role replaceRole(Role obj, long resourceVersion) throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return new Role().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Role getRole(String name) throws ApiServerException {
    try {
      Resource resource = client.get(Role.KIND, name, null);
      return new Role().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Role> listRoles() throws ApiServerException {
    try {
      List<Resource> resources = client.list(Role.KIND, null);
      List<Role> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new Role().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Executor createExecutor(Executor obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return new Executor().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteExecutor(String name) throws ApiServerException {
    try {
      client.delete(Executor.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteExecutors() throws ApiServerException {
    try {
      client.deleteList(Executor.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Executor replaceExecutor(Executor obj, long resourceVersion) throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return new Executor().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Executor getExecutor(String name) throws ApiServerException {
    try {
      Resource resource = client.get(Executor.KIND, name, null);
      return new Executor().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Executor> listExecutors() throws ApiServerException {
    try {
      List<Resource> resources = client.list(Executor.KIND, null);
      List<Executor> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new Executor().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Executor> listExecutorsByRoleName(String roleName) throws ApiServerException {
    try {
      Map<String, String> filter = Maps.newHashMap();
      filter.put(FilterKey.NAME, roleName);
      return listExecutorsByFilter(filter);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Executor> listExecutorsByState(String state) throws ApiServerException {
    try {
      Map<String, String> filter = Maps.newHashMap();
      filter.put(FilterKey.STATE, state);
      return listExecutorsByFilter(filter);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Executor> listExecutorsByFilter(Map<String, String> filter)
      throws ApiServerException {
    try {
      List<Resource> resources = client.listByFilter(Executor.KIND, filter, null);
      List<Executor> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new Executor().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Executor> listExecutorsByFilterNot(Map<String, String> filter)
      throws ApiServerException {
    try {
      List<Resource> resources = client.listByFilterNot(Executor.KIND, filter, null);
      List<Executor> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new Executor().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Worker createWorker(Worker obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return new Worker().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteWorker(String name) throws ApiServerException {
    try {
      client.delete(Worker.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteWorkers() throws ApiServerException {
    try {
      client.deleteList(Worker.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Worker replaceWorker(Worker obj, long resourceVersion) throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return new Worker().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Worker getWorker(String name) throws ApiServerException {
    try {
      Resource resource = client.get(Worker.KIND, name, null);
      return new Worker().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Worker> listWorkers() throws ApiServerException {
    try {
      List<Resource> resources = client.list(Worker.KIND, null);
      List<Worker> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new Worker().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Data createData(Data obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return new Data().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteData(String name) throws ApiServerException {
    try {
      client.delete(Data.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteDatas() throws ApiServerException {
    try {
      client.deleteList(Data.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Data replaceData(Data obj, long resourceVersion) throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return new Data().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public Data getData(String name) throws ApiServerException {
    try {
      Resource resource = client.get(Data.KIND, name, null);
      return new Data().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<Data> listDatas() throws ApiServerException {
    try {
      List<Resource> resources = client.list(Data.KIND, null);
      List<Data> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new Data().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataStream createDataStream(DataStream obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return new DataStream().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteDataStream(String name) throws ApiServerException {
    try {
      client.delete(DataStream.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteDataStreams() throws ApiServerException {
    try {
      client.deleteList(DataStream.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataStream replaceDataStream(DataStream obj, long resourceVersion)
      throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return new DataStream().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataStream getDataStream(String name) throws ApiServerException {
    try {
      Resource resource = client.get(DataStream.KIND, name, null);
      return new DataStream().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<DataStream> listDataStreams() throws ApiServerException {
    try {
      List<Resource> resources = client.list(DataStream.KIND, null);
      List<DataStream> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new DataStream().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataSource createDataSource(DataSource obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return new DataSource().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteDataSource(String name) throws ApiServerException {
    try {
      client.delete(DataSource.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteDataSources() throws ApiServerException {
    try {
      client.deleteList(DataSource.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataSource replaceDataSource(DataSource obj, long resourceVersion)
      throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return new DataSource().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataSource getDataSource(String name) throws ApiServerException {
    try {
      Resource resource = client.get(DataSource.KIND, name, null);
      return new DataSource().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<DataSource> listDataSources() throws ApiServerException {
    try {
      List<Resource> resources = client.list(DataSource.KIND, null);
      List<DataSource> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new DataSource().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataSavepoint createDataSavepoint(DataSavepoint obj) throws ApiServerException {
    try {
      Resource resource = client.create(obj.toResource());
      return new DataSavepoint().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteDataSavepoint(String name) throws ApiServerException {
    try {
      client.delete(DataSavepoint.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteDataSavepoints() throws ApiServerException {
    try {
      client.deleteList(DataSavepoint.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataSavepoint replaceDataSavepoint(DataSavepoint obj, long resourceVersion)
      throws ApiServerException {
    try {
      Resource resource = client.replace(obj.toResource(), resourceVersion);
      return new DataSavepoint().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public DataSavepoint getDataSavepoint(String name) throws ApiServerException {
    try {
      Resource resource = client.get(DataSavepoint.KIND, name, null);
      return new DataSavepoint().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public List<DataSavepoint> listDataSavepoints() throws ApiServerException {
    try {
      List<Resource> resources = client.list(DataSavepoint.KIND, null);
      List<DataSavepoint> objs = new ArrayList<>(resources.size());
      for (Resource resource : resources) {
        objs.add(new DataSavepoint().fromResource(resource));
      }
      return objs;
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public NodeAttribute createNodeAttribute(NodeAttribute nodeAttribute) throws ApiServerException {
    try {
      Resource resource = client.create(nodeAttribute.toResource());
      return new NodeAttribute().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public NodeAttribute replaceNodeAttribute(NodeAttribute nodeAttribute, long resourceVersion)
      throws ApiServerException {
    try {
      Resource resource = client.replace(nodeAttribute.toResource(), resourceVersion);
      return new NodeAttribute().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public NodeAttribute getNodeAttribute(String name) throws ApiServerException {
    try {
      Resource resource = client.get(NodeAttribute.KIND, name, null);
      return new NodeAttribute().fromResource(resource);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteNodeAttributes(NodeAttribute nodeAttribute) throws ApiServerException {
    try {
      client.deleteList(NodeAttribute.KIND);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }

  public void deleteNodeAttribute(String name) throws ApiServerException {
    try {
      client.delete(NodeAttribute.KIND, name);
    } catch (StatusRuntimeException e) {
      throw new ApiServerException(e);
    }
  }
}
