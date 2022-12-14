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

package com.bytedance.primus.common.metrics;

import com.bytedance.primus.common.metrics.impl.LogSink;
import com.bytedance.primus.common.metrics.impl.PrometheusPushGatewaySink;
import com.bytedance.primus.proto.PrimusRuntime.RuntimeConf;
import com.bytedance.primus.proto.PrimusRuntime.RuntimeConf.PrometheusPushGatewayConf;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.AtomicDouble;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimusMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(PrimusMetrics.class);
  private static final String APP_ID_TAG_NAME = "app_id";

  private static final TimerMetric emptyTimerMetric = new TimerMetric(null);
  private static final MetricRegistry metrics = new MetricRegistry();
  private static final ConcurrentHashMap<String, AtomicLong> storeValues = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, AtomicDouble> storeValuesDouble = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Timer> timerValues = new ConcurrentHashMap<>();

  private static MetricsSink sink = null;
  private static String appId = null;
  private static boolean sanitizeMetricName = false;

  public static void init(RuntimeConf runtimeConf, String appId) {
    PrimusMetrics.appId = appId;

    if (runtimeConf.hasPrometheusPushGatewayConf()) {
      PrometheusPushGatewayConf conf = runtimeConf.getPrometheusPushGatewayConf();
      sanitizeMetricName = true;
      sink = new PrometheusPushGatewaySink(
          metrics,
          conf.getHost(),
          conf.getPort()
      );
    } else {
      LOG.warn("Metrics isn't configured in RuntimeConf, default to LogSink.");
      sink = new LogSink(metrics);
    }

    sink.start();
  }

  public static SortedMap<String, Timer> getTimers(String pattern) {
    PrimusMetricFilter filter = new PrimusMetricFilter(pattern);
    return metrics.getTimers(filter);
  }

  public static SortedMap<String, Counter> getCounters(String pattern) {
    PrimusMetricFilter filter = new PrimusMetricFilter(pattern);
    return metrics.getCounters(filter);
  }

  public static SortedMap<String, Gauge> getGauges(String pattern) {
    PrimusMetricFilter filter = new PrimusMetricFilter(pattern);
    return metrics.getGauges(filter);
  }

  public static void emitCounterWithAppIdTag(String name, Map<String, String> tags, long count) {
    emitCounter(buildTaggedMetricNameWithAppId(name, tags), count);
  }

  private static void emitCounter(String name, long count) {
    if (sink == null) {
      return;
    }
    metrics.counter(name).inc(count);
  }

  public static void emitStoreWithAppIdTag(String name, Map<String, String> tags, int count) {
    emitStore(buildTaggedMetricNameWithAppId(name, tags), count);
  }

  private static void emitStore(String name, int count) {
    if (sink == null) {
      return;
    }
    storeValues.computeIfAbsent(name, k -> {
      final AtomicLong newGauge = new AtomicLong(0);
      metrics.register(name, (Gauge<Long>) newGauge::longValue);
      return newGauge;
    });
    AtomicLong gauge = storeValues.get(name);
    gauge.set(count);
  }

  public static void emitStoreWithAppIdTag(String name, Map<String, String> tags, double count) {
    emitStore(buildTaggedMetricNameWithAppId(name, tags), count);
  }

  private static void emitStore(String name, double count) {
    if (sink == null) {
      return;
    }
    storeValuesDouble.computeIfAbsent(name, k -> {
      final AtomicDouble newGauge = new AtomicDouble(0);
      metrics.register(name, (Gauge<Double>) newGauge::get);
      return newGauge;
    });
    AtomicDouble gauge = storeValuesDouble.get(name);
    gauge.set(count);
  }

  public static TimerMetric getTimerContextWithAppIdTag(String name) {
    return getTimerContext(buildTaggedMetricNameWithAppId(name, new HashMap<>()));
  }

  public static TimerMetric getTimerContextWithAppIdTag(String name, Map<String, String> tags) {
    return getTimerContext(buildTaggedMetricNameWithAppId(name, tags));
  }

  private static TimerMetric getTimerContext(String name) {
    if (sink == null) {
      return emptyTimerMetric;
    }
    Timer t = timerValues.computeIfAbsent(name, k -> {
      Timer timer = new Timer();
      metrics.register(name, timer);
      return timer;
    });
    return new TimerMetric(t.time());
  }

  public static class TimerMetric {

    private final Timer.Context context;

    public TimerMetric(Timer.Context context) {
      this.context = context;
    }

    public long stop() {
      if (context != null) {
        return context.stop();
      }
      return 0;
    }
  }

  private static class PrimusMetricFilter implements MetricFilter {

    private final String pattern;

    public PrimusMetricFilter(String pattern) {
      this.pattern = pattern;
    }

    @Override
    public boolean matches(String name, Metric metric) {
      return name.contains(pattern);
    }
  }

  public static String buildTaggedMetricNameWithAppId(String name, Map<String, String> tags) {
    String combinedTags = tags.entrySet().stream()
        .map(e -> buildTag(e.getKey(), e.getValue()))
        .reduce(
            buildTag(APP_ID_TAG_NAME, appId),
            (a, b) -> a + "," + b
        );

    return String.format("%s{%s}",
        sanitizeMetricName
            ? name.replaceAll("[^0-9a-zA-Z_]", "__")
            : name,
        combinedTags);
  }

  private static String buildTag(String name, String value) {
    return name + "=" + value;
  }
}
