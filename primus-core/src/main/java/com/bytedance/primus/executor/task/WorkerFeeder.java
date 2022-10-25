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

package com.bytedance.primus.executor.task;

import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_TASK_PERFORMANCE_EVENT;

import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.task.file.MetricToTimeLineEventBridge;
import com.bytedance.primus.executor.timeline.TimeLineConfigHelper;
import com.bytedance.primus.utils.timeline.TimelineLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerFeeder implements EventHandler<WorkerFeederEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerFeeder.class);
  public static final String EXECUTOR_TASK_RUNNER_WRITE_LATENCY = "executor.task_runner.write.latency";

  private String executorId;
  private OutputStream outputStream = null;
  private long lastTime;
  private long lastLength = 0;
  private long currentLength = 0;

  private String customThroughputMetricName;
  private MetricToTimeLineEventBridge metricToTimeLineEventBridge;
  private int port;
  private BlockingQueue<byte[]> messageQueue = null;
  private int maxNumWorkerFeederClients;
  private int maxNumPendingMessages = 100;
  private ExecutorService socketThreadPool;

  public WorkerFeeder(ExecutorContext executorContext, ServerSocketChannel serverSocketChannel,
      int port) {
    this.executorId = executorContext.getExecutorId().toString();
    this.lastTime = System.currentTimeMillis();

    if (executorContext.getPrimusExecutorConf().getPrimusConf().getCustomMetricTagsCount() > 0) {
      ArrayList<String> tags = new ArrayList<>();
      for (Map.Entry<String, String> tag : executorContext.getPrimusExecutorConf().getPrimusConf()
          .getCustomMetricTagsMap().entrySet()) {
        tags.add(tag.getKey() + "=" + tag.getValue());
      }
      customThroughputMetricName = "throughput{" + String.join(",", tags) + "}";
    } else {
      customThroughputMetricName = null;
    }

    TimelineLogger timelineLogger = executorContext.getTimelineLogger();
    int workerFeedMetricBatchSize = TimeLineConfigHelper
        .getMaxWorkerFeederWriteMetricBatchSize(timelineLogger.getTimelineConfig());
    metricToTimeLineEventBridge = new MetricToTimeLineEventBridge(timelineLogger,
        PRIMUS_TASK_PERFORMANCE_EVENT.name(), workerFeedMetricBatchSize);

    if (serverSocketChannel != null) {
      maxNumWorkerFeederClients = Math.max(1,
          executorContext.getPrimusExecutorConf().getPrimusConf().getInputManager()
              .getMaxNumWorkerFeederClients());
      LOG.info("maxNumWorkerFeederClients: " + maxNumWorkerFeederClients);
      messageQueue = new LinkedBlockingQueue<>(maxNumPendingMessages);
      socketThreadPool = Executors
          .newFixedThreadPool(maxNumWorkerFeederClients,
              new ThreadFactoryBuilder().setDaemon(true).build());
      for (int i = 0; i < maxNumWorkerFeederClients; ++i) {
        int threadId = i;
        Runnable runnable = () -> {
          try {
            LOG.info("Accepting incoming client: " + threadId);
            SocketChannel channel = serverSocketChannel.accept();
            LOG.info("Accepted incoming client: " + threadId);

            channel.configureBlocking(true);

            int sendBufferSize = channel.getOption(StandardSocketOptions.SO_SNDBUF);
            LOG.info("Socket channel sendBufferSize: " + sendBufferSize);
            // TODO: find the optimal send buffer size that maximize the throughput
            sendBufferSize = executorContext.getPrimusExecutorConf().getPrimusConf()
                .getInputManager()
                .getSocketSendBufferSize();
            if (sendBufferSize <= 0) {
              sendBufferSize = 100 * 1024 * 1024;
            }
            LOG.info("Socket channel set sendBufferSize: " + sendBufferSize);
            channel.setOption(StandardSocketOptions.SO_SNDBUF, sendBufferSize);
            sendBufferSize = channel.getOption(StandardSocketOptions.SO_SNDBUF);
            LOG.info("Socket channel sendBufferSize: " + sendBufferSize);

            List<byte[]> bytesList = new LinkedList<>();
            byte[] bytes;
            while (!Thread.currentThread().isInterrupted()) {
              bytesList.add(messageQueue.take());
              messageQueue.drainTo(bytesList);
              for (Iterator<byte[]> iter = bytesList.iterator(); iter.hasNext(); ) {
                bytes = iter.next();
                if (bytes != null) {
                  ByteBuffer buffer = ByteBuffer.wrap(bytes);
                  while (buffer.hasRemaining()) {
                    channel.write(buffer);
                  }
                  iter.remove();
                } else {
                  return;
                }
              }
            }
          } catch (IOException e) {
            LOG.info("failed to accept socket or write data: {}", e.toString());
          } catch (InterruptedException e) {
            LOG.info("failed to sleep warting for message bytes: {}", e.toString());
            Thread.currentThread().interrupt();
          }
        };
        socketThreadPool.execute(runnable);
      }
    }
    LOG.info("port: " + port);
    this.port = port;
  }

  @Override
  public void handle(WorkerFeederEvent workerFeederEvent) {
    switch (workerFeederEvent.getType()) {
      case WORKER_START:
        WorkerStartEvent workerStartEvent = (WorkerStartEvent) workerFeederEvent;
        outputStream = workerStartEvent.getOutputStream();
        break;
    }
  }

  public synchronized void feedSuccess(byte b[], int start, int length, boolean flush)
      throws InterruptedException {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        PrimusMetrics.TimerMetric latency =
            PrimusMetrics.getTimerContextWithOptionalPrefix(
                PrimusMetrics.prefixWithSingleTag(
                    EXECUTOR_TASK_RUNNER_WRITE_LATENCY, "executor_id", executorId));
        if (messageQueue != null) {
          messageQueue.put(Arrays.copyOfRange(b, start, start + length));
        } else {
          while (outputStream == null) {
            //LOG.warn("Output has not been initialized. Wait for 1 second.");
            Thread.sleep(1000);
          }
          outputStream.write(b, start, length);
          if (flush) {
            outputStream.flush();
          }
        }
        currentLength += length;
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTime > 1000) {
          int throughput = (int) ((currentLength - lastLength) / (currentTime - lastTime));
          PrimusMetrics.emitStoreWithOptionalPrefix(
              PrimusMetrics.prefixWithSingleTag("executor.throughput", "executor_id", executorId),
              throughput);
          if (customThroughputMetricName != null) {
            PrimusMetrics.emitStore(customThroughputMetricName, throughput);
          }
          lastLength = currentLength;
          lastTime = currentTime;
        }
        metricToTimeLineEventBridge.record(EXECUTOR_TASK_RUNNER_WRITE_LATENCY, latency.stop());
        metricToTimeLineEventBridge.flush();
        break;
      } catch (IOException e) {
        if ("Stream closed".equals(e.getMessage())) {
          LOG.warn("Stream closed! maybe worker exit for some reason, plz check stderr and syslog");
        } else {
          LOG.warn("WorkerFeeder write failed, try next time", e);
        }
        Thread.sleep(1000);
      }
    }
  }

  public void close() {
    try {
      if (outputStream != null) {
        LOG.info("Close worker feeder");
        outputStream.flush();
        outputStream.close();
      }
      if (metricToTimeLineEventBridge != null) {
        metricToTimeLineEventBridge.close();
      }
    } catch (IOException e) {
      LOG.warn("Failed to close worker feeder: {}", e.toString());
    }
  }
}
