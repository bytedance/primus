package com.bytedance.primus.runtime.kubernetesnative.runtime.dictionary;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class DictionaryTest {

  @Test
  public void testTranslate() {
    Dictionary dictionary = Dictionary.newDictionary(
        "app-id",
        "app-name",
        "kubernetes-namespace",
        "kubernetes-pod-name"
    );

    Map<String,String> input = new HashMap<String, String> (){{
      put("APP-ID", "{{PrimusAppId}}");
      put("APP-NAME", "{{PrimusAppName}}");
      put("K8S-NAMESPACE", "{{KubernetesNamespace}}");
      put("K8S-POD-NAME", "{{KubernetesPodName}}");
      put("NULL", null);
    }};

    Map<String,String> expected = new HashMap<String, String> () {{
      put("APP-ID", "app-id");
      put("APP-NAME", "app-name");
      put("K8S-NAMESPACE", "kubernetes-namespace");
      put("K8S-POD-NAME", "kubernetes-pod-name");
      put("NULL", null);
    }};

    Assert.assertEquals( expected, dictionary.translate(input));
  }
}
