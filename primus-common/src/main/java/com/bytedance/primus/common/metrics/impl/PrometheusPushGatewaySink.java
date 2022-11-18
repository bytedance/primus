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

package com.bytedance.primus.common.metrics.impl;

import com.bytedance.primus.common.metrics.MetricsSink;
import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusPushGatewaySink implements MetricsSink {

  private static final Logger LOG = LoggerFactory.getLogger(PrometheusPushGatewaySink.class);
  private static final String THREAD_NAME_PREFIX = "primus-prometheus-sink-thread-";
  private static final String PROMETHEUS_PUSH_GATEWAY_JOB_NAME = "primus";

  private final PushGateway pushGateway;
  private final ScheduledExecutorService executor;

  public PrometheusPushGatewaySink(
      MetricRegistry metricRegistry,
      String host,
      int port
  ) {
    this.pushGateway = new PushGateway(host + ":" + port);
    CollectorRegistry.defaultRegistry.register(
        new DropwizardExports(
            metricRegistry,
            new PrometheusPrimusSampleBuilder()
        ));

    this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, THREAD_NAME_PREFIX + threadNumber.getAndIncrement());
        t.setDaemon(true);
        return t;
      }
    });
  }

  @Override
  public void start() {
    executor.scheduleWithFixedDelay(new ReporterTask(), 15, 15, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
  }

  private void report() {
    try {
      LOG.info("Pushing metrics");
      pushGateway.push(CollectorRegistry.defaultRegistry, PROMETHEUS_PUSH_GATEWAY_JOB_NAME);
    } catch (Exception e) {
      LOG.warn(
          "Failed to push metrics to PushGateway with jobName {}: {}.",
          PROMETHEUS_PUSH_GATEWAY_JOB_NAME, e);
    }
  }

  private final class ReporterTask extends TimerTask {
    @Override
    public void run() {
      try {
        PrometheusPushGatewaySink.this.report();
      } catch (Throwable t) {
        LOG.warn("Error while reporting metrics", t);
      }
    }
  }
}
