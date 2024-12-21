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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.BinaryNode;
import com.google.common.collect.Maps;

public class SimpleRecord {
  protected final List<NameValue> values;

  public SimpleRecord() {
    this.values = new ArrayList<NameValue>();
  }

  public void add(String name, Object value) {
    values.add(new NameValue(name,value));
  }
  
  public List<NameValue> getValues() {
    return Collections.unmodifiableList(values);
  }

  public String toString() {
    return values.toString();
  }

  protected Object toJsonObject() {
    Map<String, Object> result = Maps.newLinkedHashMap();
    for (NameValue value : values) {
      result.put(value.getName(), toJsonValue(value.getValue()));
    }

    return result;
  }

  protected static Object toJsonValue(Object val) {
    if (SimpleRecord.class.isAssignableFrom(val.getClass())) {
      return ((SimpleRecord) val).toJsonObject();
    } else if (byte[].class == val.getClass()) {
      return new BinaryNode((byte[]) val);
    } else {
      return val;
    }
  }

  public static final class NameValue {
    private final String name;
    private final Object value;

    public NameValue(String name, Object value) {
      this.name = name;
      this.value = value;
    }

    public String toString() {
      return name + ": " + value;
    }

    public String getName() {
      return name;
    }

    public Object getValue() {
      return value;
    }
  }
}

