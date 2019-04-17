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

package com.bytedance.primus.common.model;

/**
 * This is the API for the applications comprising of constants that YARN sets up for the
 * applications and the containers.
 * <p>
 * TODO: Investigate the semantics and security of each cross-boundary refs.
 */


public interface ApplicationConstants {

  /**
   * The following two constants are used to expand parameter and it will be replaced with real
   * parameter expansion marker ('%' for Windows and '$' for Linux) by NodeManager on container
   * launch. For example: {{VAR}} will be replaced as $VAR on Linux, and %VAR% on Windows. User has
   * to use this constant to construct class path if user wants cross-platform practice i.e. submit
   * an application from a Windows client to a Linux/Unix server or vice versa.
   */


  public static final String PARAMETER_EXPANSION_LEFT = "{{";

  /**
   * User has to use this constant to construct class path if user wants cross-platform practice
   * i.e. submit an application from a Windows client to a Linux/Unix server or vice versa.
   */


  public static final String PARAMETER_EXPANSION_RIGHT = "}}";

  /**
   * Environment for Applications.
   * <p>
   * Some of the environment variables for applications are <em>final</em> i.e. they cannot be
   * modified by the applications.
   */
  public enum Environment {
    /**
     * $USER Final, non-modifiable.
     */
    USER("USER"),

    /**
     * $CLASSPATH
     */
    CLASSPATH("CLASSPATH"),

    /**
     * $CONTAINER_ID Final, exported by NodeManager and non-modifiable by users.
     */
    CONTAINER_ID("CONTAINER_ID"),

    /**
     * $NM_HOST Final, exported by NodeManager and non-modifiable by users.
     */
    NM_HOST("NM_HOST"),

    /**
     * $NM_HTTP_PORT Final, exported by NodeManager and non-modifiable by users.
     */
    NM_HTTP_PORT("NM_HTTP_PORT"),

    /**
     * $NM_PORT Final, exported by NodeManager and non-modifiable by users.
     */
    NM_PORT("NM_PORT");


    private final String variable;

    private Environment(String variable) {
      this.variable = variable;
    }

    public String toString() {
      return variable;
    }

    /**
     * Expand the environment variable in platform-agnostic syntax. The parameter expansion marker
     * "{{VAR}}" will be replaced with real parameter expansion marker ('%' for Windows and '$' for
     * Linux) by NodeManager on container launch. For example: {{VAR}} will be replaced as $VAR on
     * Linux, and %VAR% on Windows.
     */


    public String $$() {
      return PARAMETER_EXPANSION_LEFT + variable + PARAMETER_EXPANSION_RIGHT;
    }
  }
}
