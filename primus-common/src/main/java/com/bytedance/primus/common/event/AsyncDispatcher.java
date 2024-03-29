/*
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
 *
 * This file may have been modified by Bytedance Inc.
 */

package com.bytedance.primus.common.event;

import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.service.CompositeService;
import com.bytedance.primus.common.util.ShutdownHookManager;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that. Potentially there could be multiple channels for each event type
 * class and a thread pool can be used to dispatch the events.
 */
@SuppressWarnings("rawtypes")
public class AsyncDispatcher extends CompositeService implements Dispatcher {

  private static final Log LOG = LogFactory.getLog(AsyncDispatcher.class);

  private final BlockingQueue<Event> eventQueue;
  private volatile int lastEventQueueSizeLogged = 0;
  private volatile boolean stopped = false;

  // Configuration flag for enabling/disabling draining dispatcher's events on
  // stop functionality.
  private volatile boolean drainEventsOnStop = false;

  // Indicates all the remaining dispatcher's events on stop have been drained
  // and processed.
  // Race condition happens if dispatcher thread sets drained to true between
  // handler setting drained to false and enqueueing event. YARN-3878 decided
  // to ignore it because of its tiny impact. Also see YARN-5436.
  private volatile boolean drained = true;
  private final Object waitForDrained = new Object();

  // For drainEventsOnStop enabled only, block newly coming events into the
  // queue while stopping.
  private volatile boolean blockNewEvents = false;
  private final EventHandler<Event> handlerInstance = new GenericEventHandler();

  private Thread eventHandlingThread;
  protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
  private boolean exitOnDispatchException = true;

  private final String dispatcherName;

  private static final String DEFAULT_DISPATCHER_NAME = "AsyncDispatcher";

  public AsyncDispatcher() {
    this(DEFAULT_DISPATCHER_NAME);
  }

  /**
   * Set a name for this dispatcher.
   * @param dispatcherName name of the dispatcher thread
   */
  public AsyncDispatcher(String dispatcherName) {
    this(new LinkedBlockingQueue<Event>(), dispatcherName);
  }

  public AsyncDispatcher(BlockingQueue<Event> eventQueue,
      String dispatcherName) {
    super(dispatcherName);
    this.eventQueue = eventQueue;
    this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
    this.dispatcherName = dispatcherName;
  }

  Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          drained = eventQueue.isEmpty();
          // blockNewEvents is only set when dispatcher is draining to stop,
          // adding this check is to avoid the overhead of acquiring the lock
          // and calling notify every time in the normal run of the loop.
          if (blockNewEvents) {
            synchronized (waitForDrained) {
              if (drained) {
                waitForDrained.notify();
              }
            }
          }
          Event event;
          try {
            event = eventQueue.take();
          } catch (InterruptedException ie) {
            if (!stopped) {
              LOG.warn("AsyncDispatcher " + dispatcherName +
                  " thread " + dispatcherName + " interrupted", ie);
            }
            return;
          }
          if (event != null) {
            dispatch(event);
          }
        }
      }
    };
  }

  @VisibleForTesting
  public void disableExitOnDispatchException() {
    exitOnDispatchException = false;
  }

  @Override
  protected void serviceStart() throws Exception {
    //start all the components
    super.serviceStart();
    eventHandlingThread = new Thread(createThread());
    eventHandlingThread.setName(dispatcherName);
    eventHandlingThread.start();
  }

  public void setDrainEventsOnStop() {
    drainEventsOnStop = true;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (drainEventsOnStop) {
      blockNewEvents = true;
      LOG.info("AsyncDispatcher " + dispatcherName +
          " is draining to stop, ignoring any new events.");
      synchronized (waitForDrained) {
        while (!isDrained() && eventHandlingThread != null
            && eventHandlingThread.isAlive()) {
          waitForDrained.wait(100);
          LOG.info("Waiting for AsyncDispatcher " + dispatcherName +
              " to drain. Thread " + dispatcherName + " state is :" +
              eventHandlingThread.getState());
        }
      }
    }
    stopped = true;
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
      try {
        eventHandlingThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping", ie);
      }
    }

    // stop all the components
    super.serviceStop();
  }

  @SuppressWarnings("unchecked")
  protected void dispatch(Event event) {
    //all events go thru this loop
    if (LOG.isDebugEnabled()) {
      LOG.debug("AsyncDispatcher " + dispatcherName +
          " dispatching the event " + event.getClass().getName() + "."
          + event.toString());
    }

    Class<? extends Enum> type = event.getType().getDeclaringClass();

    try {
      EventHandler handler = eventDispatchers.get(type);
      if (handler != null) {
        handler.handle(event);
      } else {
        throw new Exception("AsyncDispatcher " + dispatcherName +
            " no handler for registered for " + type);
      }
    } catch (Throwable t) {
      //TODO Maybe log the state of the queue
      LOG.fatal("Error in dispatcher thread", t);
      // If serviceStop is called, we should exit this thread gracefully.
      if (exitOnDispatchException
          && (ShutdownHookManager.get().isShutdownInProgress()) == false
          && stopped == false) {
        stopped = true;
        Thread shutDownThread = new Thread(createShutDownThread());
        shutDownThread.setName(
            "AsyncDispatcher " + dispatcherName + " ShutDown handler");
        shutDownThread.start();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void register(Class<? extends Enum> eventType,
      EventHandler handler) {
    /* check to see if we have a listener registered */
    EventHandler<Event> registeredHandler = (EventHandler<Event>)
        eventDispatchers.get(eventType);
    LOG.info("AsyncDispatcher " + dispatcherName +
        " registering " + eventType + " for " + handler.getClass());
    if (registeredHandler == null) {
      eventDispatchers.put(eventType, handler);
    } else if (!(registeredHandler instanceof MultiListenerHandler)) {
      /* for multiple listeners of an event add the multiple listener handler */
      MultiListenerHandler multiHandler = new MultiListenerHandler();
      multiHandler.addHandler(registeredHandler);
      multiHandler.addHandler(handler);
      eventDispatchers.put(eventType, multiHandler);
    } else {
      /* already a multilistener, just add to it */
      MultiListenerHandler multiHandler
          = (MultiListenerHandler) registeredHandler;
      multiHandler.addHandler(handler);
    }
  }

  @Override
  public int eventQueueSize() {
    return eventQueue.size();
  }

  @Override
  public EventHandler<Event> getEventHandler() {
    return handlerInstance;
  }

  class GenericEventHandler implements EventHandler<Event> {

    public void handle(Event event) {
      if (blockNewEvents) {
        return;
      }
      drained = false;

      /* all this method does is enqueue all the events onto the queue */
      int qSize = eventQueue.size();
      if (qSize != 0 && qSize % 1000 == 0
          && lastEventQueueSizeLogged != qSize) {
        lastEventQueueSizeLogged = qSize;
        LOG.info("AsyncDispatcher " + dispatcherName +
            " size of event-queue is " + qSize);
      }
      int remCapacity = eventQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("AsyncDispatcher " + dispatcherName +
            " very low remaining capacity in the event-queue: " + remCapacity);
      }
      try {
        eventQueue.put(event);
      } catch (InterruptedException e) {
        if (!stopped) {
          LOG.warn("AsyncDispatcher " + dispatcherName +
              " thread " + dispatcherName + " interrupted", e);
        }
        // Need to reset drained flag to true if event queue is empty,
        // otherwise dispatcher will hang on stop.
        drained = eventQueue.isEmpty();
        throw new PrimusRuntimeException(e);
      }
    }

    ;
  }

  /**
   * Multiplexing an event. Sending it to different handlers that
   * are interested in the event.
   */
  static class MultiListenerHandler implements EventHandler<Event> {

    List<EventHandler<Event>> listofHandlers;

    public MultiListenerHandler() {
      listofHandlers = new ArrayList<EventHandler<Event>>();
    }

    @Override
    public void handle(Event event) {
      for (EventHandler<Event> handler : listofHandlers) {
        handler.handle(event);
      }
    }

    void addHandler(EventHandler<Event> handler) {
      listofHandlers.add(handler);
    }

  }

  Runnable createShutDownThread() {
    return new Runnable() {
      @Override
      public void run() {
        LOG.info("AsyncDispatcher " + dispatcherName + " exiting, bbye..");
        System.exit(-1);
      }
    };
  }

  @VisibleForTesting
  protected boolean isEventThreadWaiting() {
    return eventHandlingThread.getState() == Thread.State.WAITING;
  }

  protected boolean isDrained() {
    return drained;
  }

  protected boolean isStopped() {
    return stopped;
  }
}
