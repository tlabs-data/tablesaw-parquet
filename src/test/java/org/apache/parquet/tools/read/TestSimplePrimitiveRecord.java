/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.tools.read;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

class TestSimplePrimitiveRecord {

  class TestRecord {
    private int x;
    private int y;

    public TestRecord(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public String toString() {
      return "TestRecord {" + x + "," + y + "}";
    }
  }

  @Test
  void testBinary() {
    SimpleMapRecord r = new SimpleMapRecord();
    assertEquals("null", r.keyToString(null));
    assertEquals("true", r.keyToString(true));
    assertEquals("a", r.keyToString('a'));
    assertEquals("3.0", r.keyToString(3.0));
    assertEquals("4.0", r.keyToString(4.0f));
    assertEquals("100", r.keyToString(100));
    assertEquals("37", r.keyToString(37l));
    assertEquals("-1", r.keyToString((short) -1));
    assertEquals("test", r.keyToString("test"));
    assertEquals("123.123", r.keyToString(new BigDecimal("123.123")));
  }
}

