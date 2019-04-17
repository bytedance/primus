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

import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.apis.watch.ResourceEventHandler;
import com.bytedance.primus.apiserver.client.apis.watch.Watch;
import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.records.Meta;
import com.bytedance.primus.apiserver.records.impl.ExecutorSpecImpl;
import com.bytedance.primus.apiserver.records.impl.MetaImpl;
import com.bytedance.primus.apiserver.service.ApiServer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@FixMethodOrder()
public class TestCoreApi {

  private static final Logger LOG = LoggerFactory.getLogger(TestCoreApi.class);

  private static ApiServer apiServer;
  private static AtomicInteger addTimes;
  private static AtomicInteger updateTimes;
  private static AtomicInteger deleteTimes;

  @BeforeClass
  public static void setup() throws Exception {
    apiServer = new ApiServer();
    apiServer.start();
    addTimes = new AtomicInteger();
    updateTimes = new AtomicInteger();
    deleteTimes = new AtomicInteger();
  }

  @Test
  public void testGeneric() throws Exception {
    addTimes.set(0);
    updateTimes.set(0);
    deleteTimes.set(0);

    Client client = new DefaultClient(apiServer.getHostName(), apiServer.getPort());
    CoreApi coreApi = new CoreApi(client);
    Meta meta = new MetaImpl();
    meta.setName("chief_0");
    Executor executor = new Executor();
    executor.setMeta(meta);
    executor.setSpec(new ExecutorSpecImpl());

    Watch<Executor> watchList = coreApi.createWatchList(
        Executor.class,
        new ExecutorEventHandler("watchList_0")
    );
    // Watch interface is async so sleep a while to wait for creation
    Thread.sleep(15 * 1000);

    executor = coreApi.create(Executor.class, executor);
    executor = coreApi.get(Executor.class, Executor.KIND, executor.getMeta().getName());
    assertEquals(executor.getMeta().getName(), "chief_0");
    List<Executor> executors = coreApi.list(Executor.class, Executor.KIND);
    assertEquals(executors.size(), 1);
    assertEquals(executors.get(0).getMeta().getName(), "chief_0");

    Watch<Executor> watch = coreApi.createWatch(
        Executor.class,
        executor.getMeta().getName(),
        new ExecutorEventHandler("watch_0")
    );
    // Watch interface is async so sleep a while to wait for creation
    Thread.sleep(15 * 1000);

    coreApi.replace(Executor.class, executor, executor.getMeta().getVersion());
    executor = coreApi.get(Executor.class, Executor.KIND, executor.getMeta().getName());
    coreApi.list(Executor.class, Executor.KIND);
    coreApi.delete(Executor.KIND, executor.getMeta().getName());
    coreApi.deleteList(Executor.KIND);

    Thread.sleep(5000);
    watchList.cancel();
    watch.cancel();

    assertEquals(addTimes.get(), 1);
    assertEquals(updateTimes.get(), 2);
    assertEquals(deleteTimes.get(), 2);
  }

  @Test
  public void testExecutor() throws Exception {
    addTimes.set(0);
    updateTimes.set(0);
    deleteTimes.set(0);

    Client client = new DefaultClient(apiServer.getHostName(), apiServer.getPort());
    CoreApi coreApi = new CoreApi(client);
    Meta meta = new MetaImpl();
    meta.setName("chief_1");
    Executor executor = new Executor();
    executor.setMeta(meta);
    executor.setSpec(new ExecutorSpecImpl());

    Watch<Executor> watchList = coreApi.createWatchList(
        Executor.class,
        new ExecutorEventHandler("watchList_1")
    );
    // Watch interface is async so sleep a while to wait for creation
    Thread.sleep(15 * 1000);

    executor = coreApi.createExecutor(executor);
    executor = coreApi.getExecutor(executor.getMeta().getName());
    assertEquals(executor.getMeta().getName(), "chief_1");
    List<Executor> executors = coreApi.listExecutors();
    assertEquals(executors.size(), 1);
    assertEquals(executors.get(0).getMeta().getName(), "chief_1");

    Watch<Executor> watch = coreApi.createWatch(
        Executor.class,
        executor.getMeta().getName(),
        new ExecutorEventHandler("watch_1")
    );
    // Watch interface is async so sleep a while to wait for creation
    Thread.sleep(15 * 1000);

    coreApi.replaceExecutor(executor, executor.getMeta().getVersion());
    executor = coreApi.getExecutor(executor.getMeta().getName());
    coreApi.listExecutors();
    coreApi.deleteExecutor(executor.getMeta().getName());
    coreApi.deleteExecutors();

    Thread.sleep(5000);
    watchList.cancel();
    watch.cancel();

    assertEquals(addTimes.get(), 1);
    assertEquals(updateTimes.get(), 2);
    assertEquals(deleteTimes.get(), 2);
  }

  @AfterClass
  public static void cleanup() {
    apiServer.stop();
  }

  class ExecutorEventHandler implements ResourceEventHandler<Executor> {

    private String name;

    public ExecutorEventHandler(String name) {
      this.name = name;
    }

    @Override
    public void onAdd(Executor obj) {
      LOG.info(name + " onAdd: " + obj);
      addTimes.addAndGet(1);
    }

    @Override
    public void onUpdate(Executor oldObj, Executor newObj) {
      LOG.info(name + " onUpdate " + oldObj + " -> " + newObj);
      updateTimes.addAndGet(1);
    }

    @Override
    public void onDelete(Executor obj) {
      LOG.info(name + " onDelete " + obj);
      deleteTimes.addAndGet(1);
    }

    @Override
    public void onError(Throwable throwable) {

    }
  }
}