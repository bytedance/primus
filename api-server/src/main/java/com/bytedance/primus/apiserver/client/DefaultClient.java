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

package com.bytedance.primus.apiserver.client;

import com.bytedance.primus.apiserver.proto.ResourceServiceGrpc;
import com.bytedance.primus.apiserver.proto.ResourceServiceGrpc.ResourceServiceBlockingStub;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.CreateRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.DeleteListRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.DeleteRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.GetRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.GetStatusRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ListRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ListResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReadOptions;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReplaceRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReplaceStatusRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchListRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchListResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchResponse;
import com.bytedance.primus.apiserver.records.Resource;
import com.bytedance.primus.apiserver.records.impl.ResourceImpl;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.apiserver.utils.Constants;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultClient implements Client {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultClient.class);

  private ManagedChannel managedChannel;
  private ResourceServiceGrpc.ResourceServiceBlockingStub blockingStub;
  private ResourceServiceGrpc.ResourceServiceStub asyncStub;

  public DefaultClient(String hostname, int port) throws Exception {
    managedChannel = ManagedChannelBuilder.forAddress(hostname, port)
        .maxInboundMessageSize(Constants.MAX_MESSAGE_SIZE)
        .usePlaintext(true)
        .build();
    blockingStub = ResourceServiceGrpc.newBlockingStub(managedChannel);
    asyncStub = ResourceServiceGrpc.newStub(managedChannel);
  }

  public void shutdown() {
    if (!managedChannel.isShutdown()) {
      managedChannel.shutdown();
    }
  }

  @Override
  public Resource create(Resource resource) {
    CreateRequest.Builder builder = CreateRequest.newBuilder().setResource(resource.getProto());
    return new ResourceImpl(blockingStub.create(builder.build()).getResource());
  }

  @Override
  public void delete(String resourceKind, String resourceName) {
    DeleteRequest.Builder builder = DeleteRequest.newBuilder()
        .setResourceKind(resourceKind)
        .setResourceName(resourceName);
    blockingStub.delete(builder.build());
  }

  @Override
  public void deleteList(String resourceKind) {
    DeleteListRequest.Builder builder = DeleteListRequest.newBuilder()
        .setResourceKind(resourceKind);
    blockingStub.deleteList(builder.build());
  }

  @Override
  public Resource replace(Resource resource, long resourceVersion) {
    ReplaceRequest.Builder builder = ReplaceRequest.newBuilder()
        .setResource(resource.getProto())
        .setResourceVersion(resourceVersion);
    ResourceServiceBlockingStub blockingStubWithTimeout = this.blockingStub
        .withDeadlineAfter(30, TimeUnit.SECONDS);
    return new ResourceImpl(blockingStubWithTimeout.replace(builder.build()).getResource());
  }

  @Override
  public Resource patch(Resource resource, long resourceVersion) {
    // TODO
    return null;
  }

  @Override
  public Resource get(String resourceKind, String resourceName, ReadOptions options) {
    GetRequest.Builder builder = GetRequest.newBuilder()
        .setResourceKind(resourceKind)
        .setResourceName(resourceName);
    if (options != null) {
      builder.setOptions(options);
    }
    ResourceServiceBlockingStub blockingStubWithTimeout = this.blockingStub
        .withDeadlineAfter(30, TimeUnit.SECONDS);
    return new ResourceImpl(blockingStubWithTimeout.get(builder.build()).getResource());
  }

  @Override
  public List<Resource> list(String resourceKind, ReadOptions options) {
    ListRequest.Builder builder = ListRequest.newBuilder()
        .setResourceKind(resourceKind);
    if (options != null) {
      builder.setOptions(options);
    }
    ListResponse response = blockingStub.list(builder.build());
    List resourceList = new ArrayList(response.getResourcesCount());
    for (ResourceServiceProto.Resource resource : response.getResourcesList()) {
      resourceList.add(new ResourceImpl(resource));
    }
    return resourceList;
  }

  @Override
  public List<Resource> listByFilter(String resourceKind, Map<String, String> filter,
      ReadOptions options) {
    ListRequest.Builder builder =
        ListRequest.newBuilder().setResourceKind(resourceKind).putAllQueryParameters(filter);
    if (options != null) {
      builder.setOptions(options);
    }
    ListResponse response = blockingStub.listByFilter(builder.build());
    List<Resource> resourceList = new ArrayList<>(response.getResourcesCount());
    for (ResourceServiceProto.Resource resource : response.getResourcesList()) {
      resourceList.add(new ResourceImpl(resource));
    }
    return resourceList;
  }

  @Override
  public List<Resource> listByFilterNot(String resourceKind, Map<String, String> filter,
      ReadOptions options) {
    ListRequest.Builder builder =
        ListRequest.newBuilder().setResourceKind(resourceKind).putAllQueryParameters(filter);
    if (options != null) {
      builder.setOptions(options);
    }
    ListResponse response = blockingStub.listByFilterNot(builder.build());
    List<Resource> resourceList = new ArrayList<>(response.getResourcesCount());
    for (ResourceServiceProto.Resource resource : response.getResourcesList()) {
      resourceList.add(new ResourceImpl(resource));
    }
    return resourceList;
  }

  @Override
  public StreamObserver<WatchRequest> watch(StreamObserver<WatchResponse> streamObserver)
      throws ApiServerException {
    return asyncStub.watch(streamObserver);
  }

  @Override
  public StreamObserver<WatchListRequest> watchList(
      StreamObserver<WatchListResponse> streamObserver) throws ApiServerException {
    return asyncStub.watchList(streamObserver);
  }

  @Override
  public Resource getStatus(String resourceKind, String resourceName, ReadOptions options) {
    GetStatusRequest.Builder builder = GetStatusRequest.newBuilder()
        .setResourceKind(resourceKind)
        .setResourceName(resourceName);
    if (options != null) {
      builder.setOptions(options);
    }
    return new ResourceImpl(blockingStub.getStatus(builder.build()).getResource());
  }

  @Override
  public Resource replaceStatus(Resource resource, long resourceVersion) {
    ReplaceStatusRequest.Builder builder = ReplaceStatusRequest.newBuilder()
        .setResource(resource.getProto())
        .setResourceVersion(resourceVersion);
    return new ResourceImpl(blockingStub.replaceStatus(builder.build()).getResource());
  }

  @Override
  public Resource patchStatus(Resource resource, long resourceVersion) {
    // TODO
    return null;
  }
}
