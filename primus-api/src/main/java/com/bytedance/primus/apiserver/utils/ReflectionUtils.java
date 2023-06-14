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

package com.bytedance.primus.apiserver.utils;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReflectionUtils {

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

  /**
   * Cache of constructors for each class. Pins the classes so they can't be garbage collected until
   * ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();


  public static Class<?> getClass(String clazzName) throws ClassNotFoundException {
    return Class.forName(clazzName);
  }

  /**
   * Create an object for the given class and initialize it from conf
   *
   * @param clazz class of which an object is created
   * @return a new object
   */
  public static <T> T newInstance(Class<T> clazz) {
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(clazz);
      if (meth == null) {
        meth = clazz.getDeclaredConstructor(EMPTY_ARRAY);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(clazz, meth);
      }
      return meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T newInstance(String clazzName) throws ClassNotFoundException {
    return (T) newInstance(getClass(clazzName));
  }
}
