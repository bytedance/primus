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

package com.bytedance.primus.common.child;

import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.service.AbstractService;
import com.bytedance.primus.common.util.StreamLineGenerator;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.worker.Worker;
import com.bytedance.primus.proto.PrimusRuntime.RuntimeConf;
import com.bytedance.primus.utils.concurrent.CallableExecutionUtils;
import com.bytedance.primus.utils.concurrent.CallableExecutionUtilsBuilder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChildLauncher extends AbstractService implements EventHandler<ChildLauncherEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ChildLauncher.class);
  private static final int BUFFER_SIZE_BYTES = 128 * 1024;

  private final boolean redirectChildProcessLogs;
  private volatile boolean expectedExit = true;
  private Child child;

  private Process process;
  private BufferedOutputStream processOut;
  private BufferedInputStream processErr;
  private BufferedInputStream processIn;
  private OutThread outThread;
  private ErrThread errThread;
  private WaitForThread waitForThread;
  private int workerFeederPort = -1;

  private ChildLauncher(boolean redirectChildProcessLogs) {
    super(ChildLauncher.class.getName());
    this.redirectChildProcessLogs = redirectChildProcessLogs;
  }

  private ChildLauncher(boolean redirectChildProcessLogs, int workerFeederPort) {
    this(redirectChildProcessLogs);
    this.workerFeederPort = workerFeederPort;
  }

  private static boolean getRedirectChildProcessLogs(RuntimeConf conf) {
    return conf != null && conf.getLoggerConf().getRedirectChildProcessLogs();
  }

  public ChildLauncher(RuntimeConf conf) {
    this(getRedirectChildProcessLogs(conf));
  }

  public ChildLauncher(RuntimeConf conf, int workerFeederPort) {
    this(getRedirectChildProcessLogs(conf), workerFeederPort);
  }

  public boolean isExpectedExit() {
    return expectedExit;
  }

  @Override
  public void handle(ChildLauncherEvent event) {
    switch (event.getType()) {
      case LAUNCH:
        if (child == null) {
          child = event.getChild();
          launchProcess();
        } else {
          LOG.info("Child already started");
        }
        break;
      case RESTART:
        if (child == null) {
          child = event.getChild();
        }
        stopProcess();
        launchProcess();
        break;
      case STOP:
        stopProcess();
        break;
    }
  }

  public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
    if (process != null) {
      return process.waitFor(timeout, unit);
    }
    return true;
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    stopProcess();
  }

  private void launchProcess() {
    LOG.info("Child is launching...");
    expectedExit = false;
    try {
      if (child.getLaunchPlugin() != null) {
        child.getLaunchPlugin().init();
        child.getLaunchPlugin().preStart();
      }

      String command = child.getContext().getCommand();
      boolean useWorkerFeederServer = (workerFeederPort > 0
          && command.indexOf("-example_server_port=0") > 0);
      if (useWorkerFeederServer) {
        command = command.replace("-example_server_port=0",
            "-example_server_port=" + workerFeederPort);
      }
      ProcessBuilder builder =
          new ProcessBuilder("bash", "-c", command);
      builder.environment().putAll(child.getContext().getEnvironment());
      process = builder.start();

      if (!useWorkerFeederServer) {
        processOut = new BufferedOutputStream(process.getOutputStream(), BUFFER_SIZE_BYTES);
      } else {
        processOut = null;
      }
      processIn = new BufferedInputStream(process.getInputStream(), BUFFER_SIZE_BYTES);
      processErr = new BufferedInputStream(process.getErrorStream(), BUFFER_SIZE_BYTES);
      outThread = new OutThread();
      outThread.start();
      errThread = new ErrThread();
      errThread.start();
      if (child.getLaunchPlugin() != null) {
        child.getLaunchPlugin().postStart();
      }
      child.handle(new ChildStartedEvent(processOut));
      waitForThread = new WaitForThread();
      waitForThread.start();
    } catch (Exception e) {
      String errorMsg = "Failed to launch child, " + ExceptionUtils.getFullStackTrace(e);
      child.handle(new ChildEvent(ChildEventType.LAUNCH_FAILED, errorMsg));
    }
    LOG.info("Child is launched");
  }

  public void stopProcess() {
    expectedExit = true;
    if (child != null && child.getLaunchPlugin() != null) {
      try {
        child.getLaunchPlugin().preStop();
      } catch (Exception e) {
        LOG.warn("Error occurred before stop process", e);
      }
    }
    LOG.info("Child is stopping");
    doStopChildProcess();
    LOG.info("Child is stopped");

    if (child != null && child.getLaunchPlugin() != null) {
      try {
        child.getLaunchPlugin().postStop();
      } catch (Exception e) {
        LOG.warn("Error occurred after stop process", e);
      }
    }
    LOG.info("Child is stopped");
  }

  public void doStopChildProcess() {
    Callable<Boolean> task = () -> {
      if (process != null) {
        process.destroyForcibly();
      }
      return true;
    };
    CallableExecutionUtils callableExecutionUtils = new CallableExecutionUtilsBuilder<Boolean>()
        .setCallable(task)
        .setThreadName("DoStopChildProcessThread")
        .setTimeout(1)
        .setTimeUnit(TimeUnit.MINUTES)
        .createCallableExecutionUtils();
    callableExecutionUtils.doExecuteCallableSilence();

  }

  class WaitForThread extends Thread {

    public WaitForThread() {
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        int exitCode = process.waitFor();
        LOG.info("Child exit code is " + exitCode);
        if (child instanceof Worker) {
          ExecutorContext executorContext = ((Worker) child).getExecutorContext();
          executorContext.setRunning(false);
        }
        child.handle(new ChildExitedEvent(exitCode));
      } catch (InterruptedException e) {
        String errorMsg = "Interrupted when wait for child, " + ExceptionUtils.getFullStackTrace(e);
        child.handle(new ChildEvent(ChildEventType.CHILD_INTERRUPTED, errorMsg));
      }
    }
  }

  class OutThread extends Thread {

    public OutThread() {
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        if (redirectChildProcessLogs) {
          String line;
          StreamLineGenerator generator = new StreamLineGenerator(processIn, BUFFER_SIZE_BYTES);
          while ((line = generator.getNext()) != null) {
            LOG.info(line);
          }
        } else {
          byte buffer[] = new byte[BUFFER_SIZE_BYTES];
          int size;
          while ((size = processIn.read(buffer, 0, BUFFER_SIZE_BYTES)) >= 0) {
            System.out.write(buffer, 0, size);
          }
        }
        if (processIn != null) {
          processIn.close();
          processIn = null;
        }
      } catch (Throwable t) {
        LOG.warn("Exception in OutThread, " + ExceptionUtils.getFullStackTrace(t));
        stopProcess();
      } finally {
        try {
          if (processIn != null) {
            processIn.close();
            processIn = null;
          }
        } catch (IOException e) {
          LOG.info("ChildLauncher: OutThread: {}", e.toString());
        }
      }
    }
  }

  class ErrThread extends Thread {

    public ErrThread() {
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        if (redirectChildProcessLogs) {
          String line;
          StreamLineGenerator generator = new StreamLineGenerator(processErr, BUFFER_SIZE_BYTES);
          while ((line = generator.getNext()) != null) {
            LOG.error(line);
          }
        } else {
          byte buffer[] = new byte[BUFFER_SIZE_BYTES];
          int size;
          while ((size = processErr.read(buffer, 0, BUFFER_SIZE_BYTES)) >= 0) {
            System.err.write(buffer, 0, size);
          }
        }
        if (processErr != null) {
          processErr.close();
          processErr = null;
        }
      } catch (Throwable t) {
        LOG.warn("Exception in ErrThread, " + ExceptionUtils.getFullStackTrace(t));
        stopProcess();
      } finally {
        try {
          if (processErr != null) {
            processErr.close();
            processErr = null;
          }
        } catch (IOException e) {
          LOG.info("ChildLauncher: ErrThread: {}", e.toString());
        }
      }
    }
  }
}
