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

package com.bytedance.primus.common.model.records;

import com.bytedance.primus.common.model.records.impl.pb.TokenPBImpl;
import java.nio.ByteBuffer;

/**
 * <p><code>Token</code> is the security entity used by the framework
 * to verify authenticity of any resource.</p>
 */


public abstract class Token {


  public static Token newInstance(byte[] identifier, String kind, byte[] password,
      String service) {
    Token token = new TokenPBImpl();
    token.setIdentifier(ByteBuffer.wrap(identifier));
    token.setKind(kind);
    token.setPassword(ByteBuffer.wrap(password));
    token.setService(service);
    return token;
  }

  /**
   * Get the token identifier.
   *
   * @return token identifier
   */


  public abstract ByteBuffer getIdentifier();


  public abstract void setIdentifier(ByteBuffer identifier);

  /**
   * Get the token password
   *
   * @return token password
   */


  public abstract ByteBuffer getPassword();


  public abstract void setPassword(ByteBuffer password);

  /**
   * Get the token kind.
   *
   * @return token kind
   */


  public abstract String getKind();


  public abstract void setKind(String kind);

  /**
   * Get the service to which the token is allocated.
   *
   * @return service to which the token is allocated
   */


  public abstract String getService();


  public abstract void setService(String service);

}
