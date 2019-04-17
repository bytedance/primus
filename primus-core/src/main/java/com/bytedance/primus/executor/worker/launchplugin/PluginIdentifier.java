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

public class PluginIdentifier {

  private String name;
  private String version;
  private boolean frameworkDefault;

  public PluginIdentifier(String name, String version) {
    this.name = name;
    this.version = version;
  }

  public PluginIdentifier(String name, String version, boolean frameworkDefault) {
    this.name = name;
    this.version = version;
    this.frameworkDefault = frameworkDefault;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public boolean isFrameworkDefault() {
    return frameworkDefault;
  }

  public void setFrameworkDefault(boolean frameworkDefault) {
    this.frameworkDefault = frameworkDefault;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PluginIdentifier)) {
      return false;
    }

    PluginIdentifier that = (PluginIdentifier) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    return version != null ? version.equals(that.version) : that.version == null;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (version != null ? version.hashCode() : 0);
    result = 31 * result + (frameworkDefault ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "PluginIdentifier{" +
        "name='" + name + '\'' +
        ", version='" + version + '\'' +
        ", frameworkDefault=" + frameworkDefault +
        '}';
  }
}
