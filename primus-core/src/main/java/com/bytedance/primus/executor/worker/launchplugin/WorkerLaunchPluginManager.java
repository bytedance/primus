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

import com.bytedance.primus.apiserver.proto.ResourceProto.Plugin;
import com.bytedance.primus.apiserver.proto.ResourceProto.PluginConfig;
import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.worker.WorkerContext;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerLaunchPluginManager {

  private static final Logger log = LoggerFactory.getLogger(WorkerLaunchPluginManager.class);

  public static WorkerLaunchPluginChain getWorkerLaunchPluginChain(
      ExecutorContext executorContext,
      WorkerContext workerContext) {
    ArrayList<ChildLaunchPlugin> pluginList = new ArrayList<>();

    pluginList.add(new EnvPlugin(executorContext, workerContext));

    pluginList.add(new SocketPlugin(executorContext, workerContext));

    if (executorContext.getPrimusExecutorConf().getInputManager().getGracefulShutdown()
        && executorContext.getPrimusExecutorConf().getExecutorSpec().getInputPolicy()
        .hasStreamingInputPolicy()) {
      pluginList.add(new EofPlugin(executorContext, workerContext));
    }
    pluginList.add(new PrimusConfCompatiblePlugin());

    PluginManager pluginManager = new PluginManager(executorContext, workerContext);
    PluginConfig pluginConfig = executorContext.getPrimusExecutorConf().getExecutorSpec()
        .getPluginConfig();
    fullFillFrameworkDefaultPlugins(pluginList, pluginManager, pluginConfig);
    fullFillPlugins(pluginList, pluginManager, pluginConfig);

    return new WorkerLaunchPluginChain(pluginList);
  }

  private static void fullFillFrameworkDefaultPlugins(ArrayList<ChildLaunchPlugin> pluginList,
      PluginManager pluginManager,
      PluginConfig pluginConfig) {
    List<Pair<PluginIdentifier, ChildLaunchPlugin>> allFrameworkDefaultPlugins = pluginManager
        .findAllFrameworkDefaultPlugins();
    List<PluginIdentifier> frameworkDefaultPlugins = allFrameworkDefaultPlugins.stream()
        .map(t -> t.getKey())
        .collect(Collectors.toList());
    log.info("Total framework default plugins: {}", frameworkDefaultPlugins);
    addPlugins(pluginList, allFrameworkDefaultPlugins, pluginConfig);
  }

  public static void fullFillPlugins(ArrayList<ChildLaunchPlugin> pluginList,
      PluginManager pluginManager, PluginConfig pluginConfig) {

    List<Pair<PluginIdentifier, ChildLaunchPlugin>> basicPlugins = createPlugins(pluginManager,
        pluginConfig.getBasicPluginsList());
    log.info("Current plugins count:{}, basic plugins:{}", formattedPluginName(pluginList),
        formattedPluginPair(basicPlugins));
    addPlugins(pluginList, basicPlugins, pluginConfig);

    List<Pair<PluginIdentifier, ChildLaunchPlugin>> extendPlugins = createPlugins(pluginManager,
        pluginConfig.getExtendPluginsList());
    log.info("Current plugins count:{}, extend plugins:{}", formattedPluginName(pluginList),
        formattedPluginPair(extendPlugins));
    addPlugins(pluginList, extendPlugins, pluginConfig);
  }

  public static List<Pair<PluginIdentifier, ChildLaunchPlugin>> createPlugins(
      PluginManager pluginManager,
      List<Plugin> plugins) {
    List<Pair<PluginIdentifier, ChildLaunchPlugin>> pluginList = new ArrayList<>();
    for (Plugin plugin : plugins) {
      Optional<Pair<PluginIdentifier, ChildLaunchPlugin>> targetPlugin;
      if (Strings.isNullOrEmpty(plugin.getVersion())) {
        targetPlugin = pluginManager.listPluginByName(plugin.getName()).stream().findFirst();
      } else {
        targetPlugin = pluginManager.findPluginByNameAndVersion(plugin.getName(),
            plugin.getVersion());
      }
      if (targetPlugin.isPresent()) {
        pluginList.add(targetPlugin.get());
      }
    }
    return pluginList;
  }

  public static void addPlugins(List<ChildLaunchPlugin> pluginList,
      List<Pair<PluginIdentifier, ChildLaunchPlugin>> userSpecifiedPlugins,
      PluginConfig pluginConfig) {
    if (CollectionUtils.isEmpty(userSpecifiedPlugins)) {
      return;
    }
    Set<String> disabledPluginNameSet = pluginConfig.getDisabledPluginsList().stream()
        .map(t -> t.getName()).collect(Collectors.toSet());

    Set<String> pluginSetByUser = pluginList.stream().map(plug -> plug.getClass().getName())
        .collect(Collectors.toSet());
    for (Pair<PluginIdentifier, ChildLaunchPlugin> userSpecifiedPluginKV : userSpecifiedPlugins) {
      PluginIdentifier pluginIdentifier = userSpecifiedPluginKV.getKey();
      ChildLaunchPlugin childLaunchPlugin = userSpecifiedPluginKV.getValue();

      String pluginIdentifierName = pluginIdentifier.getName();
      String pluginClassName = childLaunchPlugin.getClass().getName();
      if (pluginSetByUser.contains(pluginClassName)) {
        log.warn("Plugin: {} has already been added by System, ignore user's specification.",
            pluginClassName);
        continue;
      }

      if (disabledPluginNameSet.contains(pluginIdentifierName)) {
        log.warn("Plugin: {} has already been disabled by user",
            pluginIdentifierName);
        continue;
      }
      pluginList.add(childLaunchPlugin);
    }
  }

  private static String formattedPluginName(List<ChildLaunchPlugin> plugins) {
    if (CollectionUtils.isEmpty(plugins)) {
      return "";
    }
    return plugins.stream().map(t -> t.getClass().getSimpleName()).collect(Collectors.toList())
        .toString();
  }

  private static String formattedPluginPair(
      List<Pair<PluginIdentifier, ChildLaunchPlugin>> plugins) {
    if (CollectionUtils.isEmpty(plugins)) {
      return "";
    }
    return plugins.stream().map(t -> t.getValue().getClass().getSimpleName())
        .collect(Collectors.toList())
        .toString();
  }

}
