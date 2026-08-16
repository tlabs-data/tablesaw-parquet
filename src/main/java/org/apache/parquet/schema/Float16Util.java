package org.apache.parquet.schema;

/*-
 * #%L
 * Tablesaw-Parquet
 * %%
 * Copyright (C) 2020 - 2021 Tlabs-data
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

import org.apache.parquet.io.api.Binary;

/**
 * Utility class with public static methods to expose Float16 package protected methods.
 */
public class Float16Util {

    private Float16Util() {
        super();
    }
    
    public static float toFloat(final Binary b) {
        return Float16.toFloat(b);
    }
    
    public static short toFloat16(final float f) {
        return Float16.toFloat16(f);
    }
}
