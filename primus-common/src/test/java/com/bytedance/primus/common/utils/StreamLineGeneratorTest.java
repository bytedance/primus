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

package com.bytedance.primus.common.utils;

import com.bytedance.primus.common.util.StreamLineGenerator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class StreamLineGeneratorTest {

  @Test
  public void testEmptyStream() throws IOException {
    StreamLineGenerator generator = new StreamLineGenerator(
        new ByteArrayInputStream("".getBytes()), 128);

    Assert.assertNull(generator.getNext());
    Assert.assertNull(generator.getNext());
  }

  @Test
  public void testShortTokensWithLongBuffer() throws IOException {
    StreamLineGenerator generator = new StreamLineGenerator(
        new ByteArrayInputStream("a\nb\nc\nd".getBytes()), 128);

    Assert.assertEquals("a", generator.getNext());
    Assert.assertEquals("b", generator.getNext());
    Assert.assertEquals("c", generator.getNext());
    Assert.assertEquals("d", generator.getNext());
    Assert.assertNull(generator.getNext());
    Assert.assertNull(generator.getNext());
  }

  @Test
  public void testLongTokensWithShortBuffer() throws IOException {
    StreamLineGenerator generator = new StreamLineGenerator(
        new ByteArrayInputStream("abcd\nefgh\nijkl\nmnop".getBytes()), 1);

    Assert.assertEquals("abcd", generator.getNext());
    Assert.assertEquals("efgh", generator.getNext());
    Assert.assertEquals("ijkl", generator.getNext());
    Assert.assertEquals("mnop", generator.getNext());
    Assert.assertNull(generator.getNext());
    Assert.assertNull(generator.getNext());
  }

  @Test
  public void testConsecutiveDelimiters() throws IOException {
    StreamLineGenerator generator = new StreamLineGenerator(
        new ByteArrayInputStream("\n\n\n\n".getBytes()), 2);

    Assert.assertNull(generator.getNext());
    Assert.assertNull(generator.getNext());
  }
}
