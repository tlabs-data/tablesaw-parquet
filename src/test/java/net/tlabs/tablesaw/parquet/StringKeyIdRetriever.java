package net.tlabs.tablesaw.parquet;

/*-
 * #%L
 * Tablesaw-Parquet
 * %%
 * Copyright (C) 2020 - 2025 Tlabs-data
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.nio.charset.StandardCharsets;
import java.util.Hashtable;

import org.apache.parquet.crypto.DecryptionKeyRetriever;

// Simple key retriever, based on UTF8 strings as key identifiers
class StringKeyIdRetriever implements DecryptionKeyRetriever{

  private final Hashtable<String,byte[]> keyMap = new Hashtable<String,byte[]>();

  public void putKey(String keyId, byte[] keyBytes) {
    keyMap.put(keyId, keyBytes);
  }

  @Override
  public byte[] getKey(byte[] keyMetaData) {
    String keyId = new String(keyMetaData, StandardCharsets.UTF_8);
    return keyMap.get(keyId);
  }
}
