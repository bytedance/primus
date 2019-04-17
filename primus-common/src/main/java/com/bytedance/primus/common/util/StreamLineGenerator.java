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

package com.bytedance.primus.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.LinkedList;

/**
 * StreamLineGenerator generates a String delimited by '\n' on each single invocation of getNext()
 * and trims trailing empty lines. Once the input stream has been fully consumed, null will be
 * returned by getNext() instead.
 */
public class StreamLineGenerator {

  private final static String DELIMITER = "\n";

  private final InputStream stream;
  private final byte[] buffer;

  private boolean last_line_enclosed = true;
  private final Deque<String> lines = new LinkedList<>();

  public StreamLineGenerator(InputStream stream, int buffer_size) {
    this.stream = stream;
    this.buffer = new byte[buffer_size];
  }

  private boolean hasReadyLine() {
    return lines.size() >= 2 || lines.size() == 1 && last_line_enclosed;
  }

  public String getNext() throws IOException {
    // Return from previous results
    if (hasReadyLine()) {
      return lines.poll();
    }

    // Load new lines
    while (!hasReadyLine()) {
      // Read from stream
      int size;
      if ((size = stream.read(buffer, 0, buffer.length)) < 0) {
        last_line_enclosed = true; // Forcibly enclose the last line.
        break;
      }

      // Tokenize
      String current = new String(buffer, 0, size, StandardCharsets.UTF_8);
      String[] tokens = current.split(DELIMITER);
      for (int i = 0; i < tokens.length; ++i) {
        // Append to the last token if it's not enclosed
        if (i == 0 && !last_line_enclosed) {
          String previous = lines.pollLast();
          lines.add(previous == null ? tokens[i] : previous + tokens[i]);
        } else {
          lines.add(tokens[i]);
        }
      }

      // Mark enclosed
      last_line_enclosed = current.endsWith(DELIMITER);
    }

    return lines.poll();
  }
}
