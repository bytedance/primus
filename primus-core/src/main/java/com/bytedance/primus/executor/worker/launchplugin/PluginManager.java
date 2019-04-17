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

package com.bytedance.primus.executor.worker.launchplugin;

import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.worker.WorkerContext;
import com.google.common.reflect.ClassPath;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginManager {

  public static final String PRIMUS_EXECUTOR_PACKAGE_NAME = "com.bytedance.primus.executor";
  private Map<PluginIdentifier, ChildLaunchPlugin> pluginMap;
  private static final Logger log = LoggerFactory.getLogger(PluginManager.class);
  private ExecutorContext executorContext;
  private WorkerContext workerContext;

  public PluginManager(ExecutorContext executorContext, WorkerContext workerContext) {
    this.executorContext = executorContext;
    this.workerContext = workerContext;
    init();
  }

  private synchronized void init() {
    if (pluginMap != null) {
      return;
    }
    pluginMap = new ConcurrentHashMap<>();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try {
      for (final ClassPath.ClassInfo info : ClassPath.from(loader)
          .getTopLevelClassesRecursive(PRIMUS_EXECUTOR_PACKAGE_NAME)) {
        Class<?> aClass = info.load();
        LaunchPlugin launchPlugin = aClass.getAnnotation(LaunchPlugin.class);
        if (launchPlugin != null && ChildLaunchPlugin.class.isAssignableFrom(aClass)) {
          PluginIdentifier key = new PluginIdentifier(launchPlugin.value(), launchPlugin.version(),
              launchPlugin.frameworkDefault());
          try {
            ChildLaunchPlugin plugin = createPlugin(aClass);
            pluginMap.put(key, plugin);
          } catch (Exception ex) {
            log.info("Error when create plugin:" + key, ex);
          }
        }
      }
    } catch (IOException e) {
      log.info("Error when create plugins.", e);
    }
  }

  private ChildLaunchPlugin createPlugin(Class<?> aClass)
      throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
    Constructor<?> declaredConstructor = aClass
        .getDeclaredConstructor(ExecutorContext.class, WorkerContext.class);
    return (ChildLaunchPlugin) declaredConstructor.newInstance(executorContext, workerContext);
  }

  public Optional<Pair<PluginIdentifier, ChildLaunchPlugin>> findPluginByNameAndVersion(String name,
      String version) {
    PluginIdentifier pluginIdentifier = new PluginIdentifier(name, version);
    if (pluginMap.containsKey(pluginIdentifier)) {
      return Optional.of(new Pair<>(pluginIdentifier, pluginMap.get(pluginIdentifier)));
    }
    return Optional.empty();
  }

  public List<Pair<PluginIdentifier, ChildLaunchPlugin>> listPluginByName(String name) {
    return pluginMap.entrySet().stream()
        .filter(t -> t.getKey().getName().equalsIgnoreCase(name))
        .map(t -> new Pair<>(t.getKey(), t.getValue()))
        .collect(Collectors.toList());
  }

  public List<Pair<PluginIdentifier, ChildLaunchPlugin>> findAllFrameworkDefaultPlugins() {
    return pluginMap.entrySet().stream()
        .filter(t -> t.getKey().isFrameworkDefault())
        .map(t -> new Pair<>(t.getKey(), t.getValue()))
        .collect(Collectors.toList());
  }

}
