package com.bytedance.primus.runtime.kubernetesnative.runtime.dictionary;

import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesAMContext;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Dictionary {

  private static final String URL_FORMAT_KEY_PRIMUS_APP_ID = "\\{\\{PrimusAppId\\}\\}";
  private static final String URL_FORMAT_KEY_PRIMUS_APP_NAME = "\\{\\{PrimusAppName\\}\\}";
  private static final String URL_FORMAT_KEY_KUBERNETES_NAMESPACE = "\\{\\{KubernetesNamespace\\}\\}";
  private static final String URL_FORMAT_KEY_KUBERNETES_POD_NAME = "\\{\\{KubernetesPodName\\}\\}";

  private final ImmutableMap<String, String> mapping;

  private Dictionary(
      String appId,
      String appName,
      String kubernetesNamespace,
      String kubernetesDriverPodName
  ) {
    mapping = ImmutableMap.<String,String>builder()
        .put(URL_FORMAT_KEY_PRIMUS_APP_ID, appId)
        .put(URL_FORMAT_KEY_PRIMUS_APP_NAME, appName)
        .put(URL_FORMAT_KEY_KUBERNETES_NAMESPACE, kubernetesNamespace)
        .put(URL_FORMAT_KEY_KUBERNETES_POD_NAME, kubernetesDriverPodName)
        .build();
  }

  public static Dictionary newDictionary(
      String appId,
      String appName,
      String kubernetesNamespace,
      String kubernetesPodName
  ) {
    return new Dictionary(
        appId,
        appName,
        kubernetesNamespace,
        kubernetesPodName
    );
  }

  public static Dictionary newDriverDictionary(
      KubernetesAMContext amContext
  ) {
    return new Dictionary(
        amContext.getAppId(),
        amContext.getAppName(),
        amContext.getKubernetesNamespace(),
        amContext.getDriverPodName()
    );
  }

  public static Dictionary newExecutorDictionary(
      KubernetesAMContext amContext,
      SchedulerExecutor executor
  ) {
    return new Dictionary(
        amContext.getAppId(),
        amContext.getAppName(),
        amContext.getKubernetesNamespace(),
        executor.getContainer().getNodeId().getHost()
    );
  }

  public String translate(String template) {
    return StringUtils.genFromTemplateAndDictionary(template, mapping);
  }

  // The latter has the higher precedence.
  @SafeVarargs
  public final Map<String, String> translate(Map<String, String>... maps) {
    Map<String, String> combined = Arrays.stream(maps)
        .reduce(new HashMap<>(), (acc, cur) -> {
          acc.putAll(cur);
          return acc;
        });

    return combined
        .entrySet()
        .stream()
        .collect(
            HashMap::new,
            (acc, entry) -> acc.put(
                entry.getKey(),
                translate(entry.getValue())),
            HashMap::putAll);
  }
}
