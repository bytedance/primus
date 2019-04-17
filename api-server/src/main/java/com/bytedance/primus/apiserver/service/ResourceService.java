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

package com.bytedance.primus.apiserver.service;

import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.proto.ApiServerConfProto.ApiServerConf;
import com.bytedance.primus.apiserver.proto.ResourceServiceGrpc.ResourceServiceImplBase;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.CreateRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.CreateResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.DeleteListRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.DeleteListResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.DeleteRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.DeleteResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.GetRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.GetResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.GetStatusRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.GetStatusResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ListRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ListResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.PatchRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.PatchResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.PatchStatusRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.PatchStatusResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReadOptions;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReplaceRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReplaceResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReplaceStatusRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReplaceStatusResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchEvent;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchEvent.Type;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchListRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchListResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchResponse;
import com.bytedance.primus.apiserver.records.Meta;
import com.bytedance.primus.apiserver.records.Resource;
import com.bytedance.primus.apiserver.records.impl.MetaImpl;
import com.bytedance.primus.apiserver.records.impl.ResourceImpl;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.apiserver.service.exception.ErrorCode;
import com.bytedance.primus.apiserver.service.filter.FilterKey;
import com.bytedance.primus.apiserver.service.storage.RocksDBStorage;
import com.bytedance.primus.apiserver.service.storage.Storage;
import com.bytedance.primus.apiserver.service.watch.Watch;
import com.bytedance.primus.apiserver.service.watch.WatchBus;
import com.bytedance.primus.apiserver.service.watch.WatchList;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.google.common.collect.Lists;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceService extends ResourceServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceService.class);

  private static final int RETAIN_VERSION = 2;
  private static final String DELIMITER = "_";
  private static final String TOMBSTONE = "1";
  private static final String YARN_APP_ID = "YARN_APP_ID";

  private AtomicLong revision;
  private Map<String, Map<String, Long>> resourceLatestVersionMap;
  private Map<String, HashSet<String>> resourceLatestDeletionMap;
  private WatchBus watchBus;
  private Storage storage;

  public ResourceService(ApiServerConf conf) throws Exception {
    revision = new AtomicLong(System.currentTimeMillis());  // A valid revision is larger than 0.
    resourceLatestVersionMap = new ConcurrentHashMap<>();
    resourceLatestDeletionMap = new ConcurrentHashMap<>();
    watchBus = new WatchBus();
    storage = new RocksDBStorage(conf, RETAIN_VERSION);
    // TODO: Recover state from storage, e.g. fill resourceLatestVersionMap and resourceLatestDeletionMap
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    try {
      LOG.debug("Receive create Request, kind" + request.getResource().getKind() + ", Meta:"
          + request.getResource().toString());
      emitRequestCounterMetric("create", request.getResource().getKind());
      checkResourceNotExist(
          request.getResource().getKind(),
          request.getResource().getMeta().getName()
      );

      Resource resource = new ResourceImpl(request.getResource());
      synchronized (this) {
        Meta meta = resource.getMeta();
        meta.setCreationTime(System.currentTimeMillis());
        meta.setVersion(revision.getAndAdd(1));
        resource.setMeta(meta);
        putResourceToStorage(resource);
        updateResourceLatestVersion(resource);
        unsetResourceLatestDeletion(resource);
      }
      notifyWatchBus(Type.ADDED, null, resource);

      CreateResponse response = CreateResponse.newBuilder()
          .setResource(resource.getProto())
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = "Raise exception while create: " + request.getResource().getKind();
      if (!silentException(e)) {
        LOG.warn(message, e);
      } else {
        LOG.warn(message + "|" + e.getMessage());
      }
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    try {
      emitRequestCounterMetric("delete", request.getResourceKind());
      deleteImpl(request.getResourceKind(), request.getResourceName());
      DeleteResponse response = DeleteResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = "Raise exception while delete: " + request.getResourceKind();
      if (!silentException(e)) {
        LOG.warn(message, e);
      } else {
        LOG.warn(message + "|" + e.getMessage());
      }
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  private synchronized void deleteImpl(String resourceKind, String resourceName) throws Exception {
    if (isResourceLatestDeletion(resourceKind, resourceName)) {
      return;
    }

    long latestVersion = getResourceLatestVersion(resourceKind, resourceName);
    Resource resource = getResourceFromStorage(resourceKind, resourceName, latestVersion);
    Meta meta = resource.getMeta();
    meta.setDeletionTime(System.currentTimeMillis());
    meta.setVersion(revision.getAndAdd(1));
    resource.setMeta(meta);
    deleteResourceInStorage(resource);
    updateResourceLatestVersion(resource);
    setResourceLatestDeletion(resource);
    notifyWatchBus(Type.DELETED, null, resource);
  }

  @Override
  public void deleteList(DeleteListRequest request,
      StreamObserver<DeleteListResponse> responseObserver) {
    try {
      emitRequestCounterMetric("deleteList", request.getResourceKind());
      Map<String, Long> resourceVersionMap =
          resourceLatestVersionMap.get(request.getResourceKind());
      if (resourceVersionMap != null) {
        for (String resourceName : resourceVersionMap.keySet()) {
          deleteImpl(request.getResourceKind(), resourceName);
        }
      }

      DeleteListResponse response = DeleteListResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = "Raise exception while deletelist: " + request.getResourceKind();
      if (!silentException(e)) {
        LOG.warn(message, e);
      } else {
        LOG.warn(message + "|" + e.getMessage());
      }
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public void replace(ReplaceRequest request, StreamObserver<ReplaceResponse> responseObserver) {
    try {
      emitRequestCounterMetric("replace", request.getResource().getKind());
      checkResourceExist(
          request.getResource().getKind(),
          request.getResource().getMeta().getName()
      );
      checkResourceLatestVersion(
          request.getResource().getKind(),
          request.getResource().getMeta().getName(),
          request.getResourceVersion()
      );

      Resource oldResource = getResourceFromStorage(
          request.getResource().getKind(),
          request.getResource().getMeta().getName(),
          request.getResourceVersion()
      );

      Resource newResource = new ResourceImpl(oldResource.getProto());
      synchronized (this) {
        checkResourceLatestVersion(
            request.getResource().getKind(),
            request.getResource().getMeta().getName(),
            request.getResourceVersion()
        );
        Meta meta = newResource.getMeta();
        meta.setVersion(revision.getAndAdd(1));
        newResource.setMeta(meta);
        newResource.setSpec(request.getResource().getSpec());
        newResource.setStatus(request.getResource().getStatus());
        putResourceToStorage(newResource);
        updateResourceLatestVersion(newResource);
        unsetResourceLatestDeletion(newResource);
      }
      notifyWatchBus(Type.MODIFIED, oldResource, newResource);

      ReplaceResponse response = ReplaceResponse.newBuilder()
          .setResource(newResource.getProto())
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = "Raise exception while replace: " + request.getResource().getKind();
      if (!silentException(e)) {
        LOG.warn(message, e);
      } else {
        LOG.warn(message + "|" + e.getMessage());
      }
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public void patch(PatchRequest request, StreamObserver<PatchResponse> responseObserver) {
    // TODO
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    try {
      emitRequestCounterMetric("get", request.getResourceKind());
      long resourceVersion = getResourceVersionFromOptions(
          request.getResourceKind(),
          request.getResourceName(),
          request.getOptions()
      );
      if (resourceVersion > 0) {
        Resource resource = getResourceFromStorage(
            request.getResourceKind(),
            request.getResourceName(),
            resourceVersion
        );
        GetResponse response = GetResponse.newBuilder()
            .setResource(resource.getProto())
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } else {
        throw new ApiServerException(ErrorCode.INVALID_ARGUMENT,
            "Resource version > 0 is required");
      }
    } catch (Exception e) {
      String message = "Raise exception while get: " + request.getResourceKind();
      if (!silentException(e)) {
        LOG.warn(message, e);
      } else {
        LOG.warn(message + "|" + e.getMessage());
      }
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public void list(ListRequest request, StreamObserver<ListResponse> responseObserver) {
    try {
      emitRequestCounterMetric("list", request.getResourceKind());
      ListResponse.Builder builder = ListResponse.newBuilder();
      Map<String, Long> resourceVersionMap =
          resourceLatestVersionMap.get(request.getResourceKind());
      if (resourceVersionMap != null) {
        for (Map.Entry<String, Long> entry : resourceVersionMap.entrySet()) {
          if (!isResourceLatestDeletion(request.getResourceKind(), entry.getKey())) {
            long resourceVersion = getResourceVersionFromOptions(
                request.getResourceKind(),
                entry.getKey(),
                request.getOptions()
            );
            if (resourceVersion > 0) {
              Resource resource = getResourceFromStorage(
                  request.getResourceKind(),
                  entry.getKey(),
                  resourceVersion
              );
              builder.addResources(resource.getProto());
            }
          }
        }
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = "Raise exception while list: " + request.getResourceKind();
      if (!silentException(e)) {
        LOG.warn(message, e);
      } else {
        LOG.warn(message + "|" + e.getMessage());
      }
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public void listByFilter(ListRequest request, StreamObserver<ListResponse> responseObserver) {
    try {
      emitRequestCounterMetric("listByFilter", request.getResourceKind());
      ListResponse.Builder builder = ListResponse.newBuilder();
      List<Resource> resources = listResource(request);
      resources.stream()
          .filter(resource -> filterResource(resource, request.getQueryParametersMap()))
          .forEach(resource -> builder.addResources(resource.getProto()));
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = "Raise exception while listByFilter: " + request.getResourceKind();
      if (!silentException(e)) {
        LOG.warn(message, e);
      } else {
        LOG.warn(message + "|" + e.getMessage());
      }
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public void listByFilterNot(ListRequest request, StreamObserver<ListResponse> responseObserver) {
    try {
      emitRequestCounterMetric("listByFilterNot", request.getResourceKind());
      ListResponse.Builder builder = ListResponse.newBuilder();
      List<Resource> resources = listResource(request);
      resources.stream()
          .filter(resource -> !filterResource(resource, request.getQueryParametersMap()))
          .forEach(resource -> builder.addResources(resource.getProto()));
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = "Raise exception while listByFilterNot: " + request.getResourceKind();
      if (!silentException(e)) {
        LOG.warn(message, e);
      } else {
        LOG.warn(message + "|" + e.getMessage());
      }
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public StreamObserver<WatchRequest> watch(StreamObserver<WatchResponse> responseObserver) {
    return new StreamObserver<WatchRequest>() {

      private String watchKey;
      private long watchUid;

      @Override
      public void onNext(WatchRequest watchRequest) {
        watchKey = watchRequest.getResourceKind() + DELIMITER + watchRequest.getResourceName();
        switch (watchRequest.getWatchOp()) {
          case CREATE:
            watchUid = watchBus.createWatch(watchKey, new Watch(responseObserver));
            break;
          case CANCEL:
            watchBus.cancelWatch(watchKey, watchUid);
            break;
        }
      }

      @Override
      public void onError(Throwable throwable) {
        LOG.warn("Watch failed " + Status.fromThrowable(throwable));
        watchBus.cancelWatch(watchKey, watchUid);
      }

      @Override
      public void onCompleted() {
        watchBus.cancelWatch(watchKey, watchUid);
      }
    };
  }

  @Override
  public StreamObserver<WatchListRequest> watchList(
      StreamObserver<WatchListResponse> responseObserver) {
    return new StreamObserver<WatchListRequest>() {

      private String watchKey;
      private long watchUid;

      @Override
      public void onNext(WatchListRequest watchListRequest) {
        watchKey = watchListRequest.getResourceKind();
        switch (watchListRequest.getWatchOp()) {
          case CREATE:
            watchUid = watchBus.createWatch(watchKey, new WatchList(responseObserver));
            break;
          case CANCEL:
            watchBus.cancelWatch(watchKey, watchUid);
            break;
        }
      }

      @Override
      public void onError(Throwable throwable) {
        LOG.warn("Watch failed " + Status.fromThrowable(throwable));
        watchBus.cancelWatch(watchKey, watchUid);
      }

      @Override
      public void onCompleted() {
        watchBus.cancelWatch(watchKey, watchUid);
      }
    };
  }

  @Override
  public void getStatus(GetStatusRequest request,
      StreamObserver<GetStatusResponse> responseObserver) {
    try {
      emitRequestCounterMetric("getStatus", request.getResourceKind());
      long resourceVersion = getResourceVersionFromOptions(
          request.getResourceKind(),
          request.getResourceName(),
          request.getOptions()
      );
      if (resourceVersion > 0) {
        Resource resource = getResourceFromStorage(
            request.getResourceKind(),
            request.getResourceName(),
            resourceVersion
        );
        Resource newResource = new ResourceImpl();
        newResource.setMeta(new MetaImpl(resource.getMeta().getProto()));
        newResource.setStatus(resource.getStatus());

        GetStatusResponse response = GetStatusResponse.newBuilder()
            .setResource(newResource.getProto())
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } else {
        throw new ApiServerException(ErrorCode.INVALID_ARGUMENT,
            "Resource version > 0 is required");
      }
    } catch (Exception e) {
      LOG.warn(ExceptionUtils.getFullStackTrace(e));
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public void replaceStatus(ReplaceStatusRequest request,
      StreamObserver<ReplaceStatusResponse> responseObserver) {
    try {
      emitRequestCounterMetric("replaceStatus", request.getResource().getKind());
      checkResourceExist(
          request.getResource().getKind(),
          request.getResource().getMeta().getName()
      );
      checkResourceLatestVersion(
          request.getResource().getKind(),
          request.getResource().getMeta().getName(),
          request.getResourceVersion()
      );

      Resource oldResource = getResourceFromStorage(
          request.getResource().getKind(),
          request.getResource().getMeta().getName(),
          request.getResourceVersion()
      );

      Resource newResource = new ResourceImpl(oldResource.getProto());
      synchronized (this) {
        checkResourceLatestVersion(
            request.getResource().getKind(),
            request.getResource().getMeta().getName(),
            request.getResourceVersion()
        );
        Meta meta = newResource.getMeta();
        meta.setVersion(revision.getAndAdd(1));
        newResource.setMeta(meta);
        newResource.setStatus(request.getResource().getStatus());
        putResourceToStorage(newResource);
        updateResourceLatestVersion(newResource);
        unsetResourceLatestDeletion(newResource);
      }
      notifyWatchBus(Type.MODIFIED, oldResource, newResource);

      ReplaceStatusResponse response = ReplaceStatusResponse.newBuilder()
          .setResource(newResource.getProto())
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.warn(ExceptionUtils.getFullStackTrace(e));
      responseObserver.onError(new ApiServerException(ExceptionUtils.getFullStackTrace(e), e));
    }
  }

  @Override
  public void patchStatus(PatchStatusRequest request,
      StreamObserver<PatchStatusResponse> responseObserver) {
    // TODO
  }

  private void notifyWatchBus(Type watchEventType, Resource oldResource, Resource newResource)
      throws Exception {
    WatchEvent.Builder builder = WatchEvent.newBuilder()
        .setType(watchEventType);
    if (oldResource != null) {
      builder.setOldResource(oldResource.getProto());
    }
    builder.setNewResource(newResource.getProto());
    WatchEvent watchEvent = builder.build();
    // Notify watch and watchList
    watchBus.post(newResource.getKind(), watchEvent);
    watchBus.post(newResource.getKind() + DELIMITER + newResource.getMeta().getName(), watchEvent);
  }

  private void putResourceToStorage(Resource resource) throws Exception {
    storage.put(
        resource.getKind(),
        resource.getMeta().getName(),
        resource.getMeta().getVersion(),
        encodeToStorageValue(resource)
    );
  }

  private void deleteResourceInStorage(Resource resource) throws Exception {
    storage.put(
        resource.getKind(),
        resource.getMeta().getName(),
        resource.getMeta().getVersion(),
        encodeToStorageValue(resource, true)
    );
  }

  private Resource getResourceFromStorage(String resourceKind, String resourceName, long version)
      throws Exception {
    byte[] bytes = storage.get(resourceKind, resourceName, version);
    return new ResourceImpl(bytes);
  }

  private byte[] encodeToStorageValue(Resource resource) throws ApiServerException {
    return encodeToStorageValue(resource, false);
  }

  private byte[] encodeToStorageValue(Resource resource, boolean deletion)
      throws ApiServerException {
    byte[] bytes = resource.getProto().toByteArray();
    if (deletion) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(bytes);
        bos.write(DELIMITER.getBytes());
        bos.write(TOMBSTONE.getBytes());
        return bos.toByteArray();
      } catch (IOException e) {
        throw new ApiServerException(ErrorCode.INTERNAL,
            "Api server failed to create key of storage from resource", e);
      }
    } else {
      return bytes;
    }
  }

  private void updateResourceLatestVersion(Resource resource) {
    synchronized (resourceLatestVersionMap) {

      resourceLatestVersionMap.putIfAbsent(resource.getKind(), new ConcurrentHashMap<>());
      resourceLatestVersionMap.get(resource.getKind())
          .put(resource.getMeta().getName(), resource.getMeta().getVersion());
    }
  }

  private long getResourceLatestVersion(String resourceKind, String resourceName) {
    Map<String, Long> resourceVersionMap = resourceLatestVersionMap.get(resourceKind);
    if (resourceVersionMap != null) {
      return resourceVersionMap.getOrDefault(resourceName, -1L);
    }
    return -1L;
  }

  private boolean isResourceLatestDeletion(String resourceKind, String resourceName) {
    HashSet<String> resourceNameSet = resourceLatestDeletionMap.get(resourceKind);
    if (resourceNameSet != null) {
      if (resourceNameSet.contains(resourceName)) {
        return true;
      }
    }
    return false;
  }

  private long getResourceVersionFromOptions(String resourceKind, String resourceName,
      ReadOptions options) {
    long resourceVersion = -1;
    switch (options.getResourceVersionMatch()) {
      case EXACT:
        resourceVersion = options.getResourceVersion();
        break;
      case LATEST:
      default:
        Map<String, Long> resourceVersionMap = resourceLatestVersionMap.get(resourceKind);
        if (resourceVersionMap != null) {
          resourceVersion = resourceVersionMap.get(resourceName);
        }
        break;
    }
    return resourceVersion;
  }

  void setResourceLatestDeletion(Resource resource) {
    synchronized (resourceLatestDeletionMap) {
      resourceLatestDeletionMap.putIfAbsent(resource.getKind(), new HashSet<>());
      resourceLatestDeletionMap.get(resource.getKind()).add(resource.getMeta().getName());
    }
  }

  void unsetResourceLatestDeletion(Resource resource) {
    HashSet<String> deletions = resourceLatestDeletionMap.get(resource.getKind());
    if (deletions != null) {
      deletions.remove(resource.getMeta().getName());
    }
  }

  private void checkResourceExist(String resourceKind, String resourceName)
      throws ApiServerException {
    long latestVersion = getResourceLatestVersion(resourceKind, resourceName);
    if (latestVersion <= 0 || isResourceLatestDeletion(resourceKind, resourceName)) {
      throw new ApiServerException(ErrorCode.NOT_FOUND,
          "Resource [" + resourceKind + ", " + resourceName + "] not found");
    }
  }

  private void checkResourceNotExist(String resourceKind, String resourceName)
      throws ApiServerException {
    long latestVersion = getResourceLatestVersion(resourceKind, resourceName);
    if (latestVersion > 0 && !isResourceLatestDeletion(resourceKind, resourceName)) {
      throw new ApiServerException(ErrorCode.ALREADY_EXISTS,
          "Resource [" + resourceKind + ", " + resourceName + "] already exists");
    }
  }

  private void checkResourceLatestVersion(String resourceKind, String resourceName,
      long resourceVersion) throws ApiServerException {
    long latestVersion = getResourceLatestVersion(resourceKind, resourceName);
    if (latestVersion != resourceVersion) {
      throw new ApiServerException(ErrorCode.PERMISSION_DENIED,
          "Not allow operation without providing the latest version of resource [ " + resourceName
              + " ], required "
              + latestVersion + ", provided " + resourceVersion);
    }
  }

  private void emitRequestCounterMetric(String api, String kind) {
    PrimusMetrics.emitCounterWithOptionalPrefix(
        "apiserver." + api + "_count{kind=" + kind + "}",
        1
    );
  }

  private String preprocessResourceName(Resource resource) {
    String name = resource.getMeta().getName();
    switch (resource.getKind()) {
      case Executor.KIND:
        if (name.startsWith("executor_")) {
          String[] names = name.split("_");
          if (names.length >= 4) {
            return String.join("_", Arrays.copyOfRange(names, 1, names.length - 2));
          }
        }
        return name;
      default:
        return name;
    }
  }

  private String preprocessResourceState(Resource resource) {
    String state = "";
    try {
      switch (resource.getKind()) {
        case Executor.KIND:
          Executor executor = new Executor().fromResource(resource);
          if (StringUtils.isEmpty(executor.getStatus().getState())) {
            executor.getStatus().setState("NONE");
            resource.setStatus(executor.toResource().getStatus());
          }
          state = executor.getStatus().getState();
          break;
        default:
      }
    } catch (ApiServerException e) {
      LOG.error("preprocessResourceState " + resource.getKind() + " Failed! ", e);
    }
    return state;
  }

  private List<Resource> listResource(ListRequest request) throws Exception {
    List<Resource> ret = Lists.newArrayList();
    Map<String, Long> resourceVersionMap = resourceLatestVersionMap.get(request.getResourceKind());
    if (resourceVersionMap != null) {
      for (Map.Entry<String, Long> entry : resourceVersionMap.entrySet()) {
        if (!isResourceLatestDeletion(request.getResourceKind(), entry.getKey())) {
          long resourceVersion =
              getResourceVersionFromOptions(
                  request.getResourceKind(), entry.getKey(), request.getOptions());
          if (resourceVersion > 0) {
            Resource resource =
                getResourceFromStorage(request.getResourceKind(), entry.getKey(), resourceVersion);
            ret.add(resource);
          }
        }
      }
    }
    return ret;
  }

  private boolean filterResource(Resource resource, Map<String, String> filter) {
    boolean ret = true;
    for (String key : filter.keySet()) {
      switch (key) {
        case FilterKey.NAME:
          ret = ret && filter.get(key).equals(preprocessResourceName(resource));
          break;
        case FilterKey.STATE:
          ret = ret && filter.get(key).equals(preprocessResourceState(resource));
          break;
        default:
          return false;
      }
    }
    return ret;
  }

  private boolean silentException(Exception e) {
    if (e instanceof ApiServerException) {
      ErrorCode errorCode = ((ApiServerException) e).getErrorCode();
      return errorCode == ErrorCode.PERMISSION_DENIED;
    }
    return false;
  }
}
