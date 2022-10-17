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

package com.bytedance.primus.am.datastream.file;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMasterEvent;
import com.bytedance.primus.am.ApplicationMasterEventType;
import com.bytedance.primus.am.datastream.file.operator.FileOperator;
import com.bytedance.primus.am.datastream.file.operator.Input;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.apiserver.records.DataSourceSpec;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.proto.PrimusCommon.DayFormat;
import com.bytedance.primus.proto.PrimusCommon.Time.TimeCase;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import com.bytedance.primus.utils.FileUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileScanner {

  private static final Duration SCAN_TO_NOW_PERIOD = Duration.ofMinutes(5);

  private final Logger LOG;
  private final AMContext context;

  private final DataStreamSpec dataStreamSpec;
  private final FileOperator fileOperator;
  private final BlockingQueue<List<Input>> inputQueue;
  private final FileSystem fileSystem;
  private final Thread scannerThread;
  private volatile boolean isStopped = false;
  private volatile boolean isFinished = false; // TODO: move to !scannerThread.isAlive()

  public FileScanner(AMContext context, String name, DataStreamSpec dataStreamSpec,
      FileOperator fileOperator) throws IOException {
    this.LOG = LoggerFactory.getLogger(FileScanner.class.getName() + "[" + name + "]");
    this.context = context;
    this.dataStreamSpec = dataStreamSpec;
    this.fileOperator = fileOperator;
    inputQueue = new LinkedBlockingQueue<>();
    fileSystem = FileSystem.get(context.getHadoopConf());
    scannerThread = new ScannerThread();
    scannerThread.start();
  }

  public BlockingQueue<List<Input>> getInputQueue() {
    return inputQueue;
  }

  private void processAndAddToQueue(List<Input> inputs) {
    if (!inputs.isEmpty()) {
      try {
        List<Pair<String, List<Input>>> pairs = fileOperator.apply(inputs);
        for (Pair<String, List<Input>> pair : pairs) {
          inputQueue.add(pair.getValue());
        }
      } catch (Exception e) {
        LOG.error("FileOperator catches error", e);
      }
    }
  }

  private void checkFileAccessibility(
      FileSystem fileSystem,
      FileSourceInput fileSourceInput
  ) throws IOException {
    try {
      fileSystem.exists(new Path(fileSourceInput.getSpec().getFilePath()));
    } catch (AccessControlException e) {
      String diag = "Failed to check access input["
          + fileSourceInput.getSpec().getFilePath()
          + "], fail job because of " + e;
      failedApp(diag, ApplicationMasterEventType.FAIL_APP,
          ApplicationExitCode.GDPR.getValue());
    } catch (IllegalArgumentException e) {
      String diag = "Failed to check access input["
          + fileSourceInput.getSpec().getFilePath()
          + "], fail job because of " + e;
      failedApp(diag, ApplicationMasterEventType.FAIL_APP,
          ApplicationExitCode.WRONG_FS.getValue());
    }
  }

  private List<FileSourceInput> buildFileSourceInputs(DataStreamSpec dataStreamSpec) {
    return dataStreamSpec.getDataSourceSpecs().stream()
        .map(this::buildFileSourceInput)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private FileSourceInput buildFileSourceInput(DataSourceSpec dataSourceSpec) {
    switch (dataSourceSpec.getProto().getDataSourceCase()) {
      case FILESOURCESPEC: {
        FileSourceSpec fss = dataSourceSpec.getFileSourceSpec();
        return new FileSourceInput(
            dataSourceSpec.getSourceId(),
            dataSourceSpec.getSource(),
            fss);
      }
      default:
        LOG.warn("Unsupported source spec " + dataSourceSpec.getProto().getDataSourceCase());
        return null;
    }
  }

  // TODO: allow different settings for each fileSourceInput
  private boolean isInputWithTimeRange(List<FileSourceInput> fileSourceInputs) {
    Optional<FileSourceInput> first = fileSourceInputs.stream().findFirst();
    if (!first.isPresent()) {
      return false;
    }

    boolean withTimeRange = first.get().getTimeRange().isPresent();
    for (FileSourceInput input : fileSourceInputs) {
      Preconditions.checkArgument(withTimeRange == input.getTimeRange().isPresent());
    }
    return withTimeRange;
  }

  private static boolean isDayGranularity(FileSourceInput fileSourceInput) {
    Optional<TimeRange> timeRange = fileSourceInput.getTimeRange();
    return timeRange.isPresent() && timeRange.get().getFrom().getTimeCase() == TimeCase.DATE;
  }

  private List<Input> scanFileSourceInputWithKey(List<FileSourceInput> fileSourceInputs) {
    List<Input> inputs = new LinkedList<>();
    for (FileSourceInput fileSourceInput : fileSourceInputs) {
      inputs.add(
          new PrimusInput(
              fileSourceInput.getSourceId(),
              fileSourceInput.getSource(),
              fileSourceInput.getSource(), // Reuse for key
              fileSourceInput.getSpec().getFilePath(),
              fileSourceInput.getSpec()
          )
      );
    }
    return inputs;
  }

  private List<Input> scanFileSourceInput(
      boolean hasDayGranularity,
      boolean checkSuccess,
      FileSourceInput input
  ) throws RuntimeException {
    try {
      checkFileAccessibility(fileSystem, input);
      Pair<Integer, Integer> startDateHour = input.getStartDateHour();
      Pair<Integer, Integer> endDateHour = input.getEndDateHour();
      return scanFileSourceInput(
          fileSystem, input, isDayGranularity(input),
          startDateHour.getKey(), startDateHour.getValue(),
          endDateHour.getKey(), endDateHour.getValue(),
          input.getSpec().getDayFormat(),
          hasDayGranularity, checkSuccess);
    } catch (IOException | ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private List<Input> scanFileSourceInput(
      FileSystem fileSystem,
      FileSourceInput fileSourceInput,
      boolean isDayGranularity,
      Integer startDay,
      Integer startHour,
      Integer endDay,
      Integer endHour,
      DayFormat dayFormat,
      boolean dayKey,
      boolean checkSuccess) throws IOException, ParseException {
    TimerMetric latency = PrimusMetrics
        .getTimerContextWithOptionalPrefix("am.filescanner.scan_inputs.latency");
    try {
      if (isDayGranularity) {
        return FileUtils.scanDayInput(fileSystem, fileSourceInput, startDay, endDay,
            dayFormat, checkSuccess);
      } else {
        return FileUtils.scanHourInput(fileSystem, fileSourceInput, startDay, startHour,
            endDay, endHour, dayFormat, dayKey, checkSuccess, context.getHadoopConf());
      }
    } finally {
      latency.stop();
    }
  }

  private void failedApp(String diag, ApplicationMasterEventType eventType, int exitCode) {
    LOG.error(diag);
    context.getDispatcher().getEventHandler()
        .handle(new ApplicationMasterEvent(context, eventType, diag, exitCode));
  }

  class ScannerThread extends Thread {

    public ScannerThread() {
      super(ScannerThread.class.getName());
      setDaemon(true);
    }

    private void scanFileSourceInputWithTimeRange(
        List<FileSourceInput> inputs
    ) throws ParseException {

      TimeRangeIterator iterator = new TimeRangeIterator(inputs);
      boolean hasDayGranularity = inputs.stream().anyMatch(FileScanner::isDayGranularity);
      // TODO: use a window to instruct scanFileSourceInput for checking _SUCCESS files.
      boolean checkSuccess = false;  // not check success file of existing data.

      // TODO: break the batch logic to prevent rescanning the entire batch due to a single
      //  failure among them. Also, more responsive to receiving isStopped.
      while (!isStopped && iterator.prepareNextBatch()) {
        try {
          List<FileSourceInput> fileSourceInputs = iterator.peekNextBatch();
          List<Input> scannedInputs = fileSourceInputs.stream()
              .flatMap(in -> scanFileSourceInput(hasDayGranularity, checkSuccess, in).stream())
              .collect(Collectors.toList());

          processAndAddToQueue(scannedInputs);
          LOG.info("{} input(s) has been enqueued, range: [{}, {}]",
              scannedInputs.size(),
              iterator.getNextBatchStartTime(),
              iterator.getNextBatchEndTime());

          // An early check to prevent the subsequent long sleep.
          if (isStopped || !iterator.prepareNextBatch()) {
            break;
          }

        } catch (RuntimeException e) {
          // TODO: Retry on scanning and processing errors
          LOG.warn("Error when scanning some FileSourceInput: %s", e);
        } finally {
          iterator.popNextBatch();
        }

        // Moving on to the next batch
        Sleeper.sleepWithoutInterruptedException(SCAN_TO_NOW_PERIOD);
      }
    }

    @Override
    public void run() {
      List<FileSourceInput> pendingInputs = buildFileSourceInputs(dataStreamSpec);

      // Input with TimeRange
      if (isInputWithTimeRange(pendingInputs)) {
        try { // TODO: Create DateHour class to eliminate the excessive exception handling.
          scanFileSourceInputWithTimeRange(pendingInputs);
        } catch (ParseException e) {
          failedApp(String.format("Failed to iterate pendingInputs: %s, e)", e),
              ApplicationMasterEventType.FAIL_APP,
              ApplicationExitCode.DATA_INCOMPATIBLE.getValue());
        }
        isFinished = true;
        return;
      }

      // Basic input
      List<Input> inputs = scanFileSourceInputWithKey(pendingInputs);
      processAndAddToQueue(inputs);
      isFinished = true;
    }
  }

  public boolean isFinished() {
    return isFinished;
  }

  public void stop() {
    isStopped = true;
    scannerThread.interrupt();
    try {
      scannerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Stop caught interrupted exception", e);
    }
  }
}
