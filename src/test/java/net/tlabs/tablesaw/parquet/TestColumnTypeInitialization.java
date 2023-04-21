package net.tlabs.tablesaw.parquet;

/*-
 * #%L
 * Tablesaw-Parquet
 * %%
 * Copyright (C) 2020 - 2023 Tlabs-data
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

import java.io.File;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;

class TestColumnTypeInitialization {

    @Test
    void testColumnTypeNotInitializedBug() {
        //tech.tablesaw.api.ColumnType.values(); // Workaround
        Table table = Table.create("t").addColumns(DoubleColumn.create("c", 1, 2, 3));
        new TablesawParquetWriter().write(table, TablesawParquetWriteOptions.builder("target/test/results/test.parquet").build());
        assertTrue(new File("target/test/results/test.parquet").exists(), "File not written");
    }

}
