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

package com.bytedance.primus.apiserver.client.apis.watch;

import com.bytedance.primus.apiserver.client.Client;
import com.bytedance.primus.apiserver.client.models.ApiType;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchEvent;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchListResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchOp;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchListRequest;
import com.bytedance.primus.apiserver.records.Resource;
import com.bytedance.primus.apiserver.records.impl.ResourceImpl;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.apiserver.utils.ReflectionUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Watch<T extends ApiType> {

  private static final Logger LOG = LoggerFactory.getLogger(Watch.class);

  private StreamObserver<WatchRequest> watchRequestStreamObserver;
  private StreamObserver<WatchListRequest> watchListRequestStreamObserver;
  private String resourceKind;
  private String resourceName;

  public Watch(
      Client client,
      Class<T> tClass,
      ResourceEventHandler<T> resourceEventHandler) throws ApiServerException {
    this.watchListRequestStreamObserver = client.watchList(
        new StreamObserver<WatchListResponse>() {
          @Override
          public void onNext(WatchListResponse response) {
            onNextImpl(tClass, response.getWatchEvent(), resourceEventHandler);
          }

          @Override
          public void onError(Throwable throwable) {
            resourceEventHandler.onError(throwable);
          }

          @Override
          public void onCompleted() {

          }
        }
    );

    try {
      this.resourceKind = tClass.getConstructor().newInstance().getKind();
      WatchListRequest request = WatchListRequest.newBuilder()
          .setResourceKind(resourceKind)
          .build();
      watchListRequestStreamObserver.onNext(request);
    } catch (ReflectiveOperationException e) {
      throw new ApiServerException(e);
    }
  }

  public Watch(
      Client client,
      Class<T> tClass,
      String resourceName,
      ResourceEventHandler<T> resourceEventHandler) throws ApiServerException {
    watchRequestStreamObserver = client.watch(
        new StreamObserver<WatchResponse>() {
          @Override
          public void onNext(WatchResponse response) {
            onNextImpl(tClass, response.getWatchEvent(), resourceEventHandler);
          }

          @Override
          public void onError(Throwable throwable) {
            resourceEventHandler.onError(throwable);
          }

          @Override
          public void onCompleted() {

          }
        }
    );

    try {
      this.resourceKind = tClass.getConstructor().newInstance().getKind();
      this.resourceName = resourceName;
      WatchRequest request = WatchRequest.newBuilder()
          .setWatchOp(WatchOp.CREATE)
          .setResourceKind(resourceKind)
          .setResourceName(resourceName)
          .build();
      watchRequestStreamObserver.onNext(request);
    } catch (ReflectiveOperationException e) {
      throw new ApiServerException(e);
    }
  }

  private void onNextImpl(
      Class<T> tClass,
      WatchEvent event,
      ResourceEventHandler<T> resourceEventHandler) {
    try {
      Resource resource = new ResourceImpl(event.getNewResource());
      T obj = (T) ReflectionUtils.newInstance(tClass).fromResource(resource);
      switch (event.getType()) {
        case ADDED:
          resourceEventHandler.onAdd(obj);
          break;
        case MODIFIED:
          Resource oldResource = new ResourceImpl(event.getOldResource());
          T oldObj = (T) ReflectionUtils.newInstance(tClass).fromResource(oldResource);
          resourceEventHandler.onUpdate(oldObj, obj);
          break;
        case DELETED:
          resourceEventHandler.onDelete(obj);
          break;
      }
    } catch (Exception e) {
      resourceEventHandler.onError(e);
    }
  }

  public void cancel() {
    if (watchRequestStreamObserver != null) {
      WatchRequest request = WatchRequest.newBuilder()
          .setWatchOp(WatchOp.CANCEL)
          .setResourceKind(resourceKind)
          .setResourceName(resourceName)
          .build();
      watchRequestStreamObserver.onNext(request);
      watchRequestStreamObserver.onCompleted();
    }
    if (watchListRequestStreamObserver != null) {
      WatchListRequest request = WatchListRequest.newBuilder()
          .setWatchOp(WatchOp.CANCEL)
          .setResourceKind(resourceKind)
          .build();
      watchListRequestStreamObserver.onNext(request);
      watchListRequestStreamObserver.onCompleted();
    }
  }
}