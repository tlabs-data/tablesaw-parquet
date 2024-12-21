package net.tlabs.tablesaw.parquet;

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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tech.tablesaw.api.Table;
import tech.tablesaw.io.DataFrameReader;
import tech.tablesaw.io.DataFrameWriter;

class TestTablesawRegistry {

    private static final String PANDAS_PYARROW = "target/test-classes/pandas_pyarrow.parquet";
    private static final String WRONG_PARQUET_FILE = "target/test-classes/thisisnota.parquet";
    private static final String OUTPUT_FILE = "target/test/results/out.parquet";

    @BeforeAll
    static void init() {
        TablesawParquet.register();
    }

    @Test
    void testRegistryReadFilename() throws IOException {
        final Table table = Table.read().file(PANDAS_PYARROW);
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadFile() throws IOException {
        final Table table = Table.read().file(new File(PANDAS_PYARROW));
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadMissingFile() throws IOException {
        final File file = new File(WRONG_PARQUET_FILE);
        final DataFrameReader dfr = Table.read();
        assertThrows(IllegalStateException.class, () -> dfr.file(file));
    }

    @Test
    void testRegistryReadUsingOptionsBuilder() throws IOException {
        final Table table = Table.read().usingOptions(TablesawParquetReadOptions.builder(PANDAS_PYARROW));
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadUsingOptions() throws IOException {
        final Table table = Table.read().usingOptions(TablesawParquetReadOptions.builder(PANDAS_PYARROW).build());
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadString() {
        final DataFrameReader dfr = Table.read();
        assertThrows(UnsupportedOperationException.class, () -> dfr.string("STRING", "parquet"));
    }

    @Test
    void testRegistryReadURL() throws IOException {
        final Table table = Table.read().url(new File(PANDAS_PYARROW).toURI().toURL());
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadURLUsingOptions() throws IOException {
        final Table table = Table.read()
            .usingOptions(TablesawParquetReadOptions.builder(new File(PANDAS_PYARROW).toURI().toURL()));
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadURIUsingOptions() throws IOException {
        final Table table = Table.read()
            .usingOptions(TablesawParquetReadOptions.builder(new File(PANDAS_PYARROW).toURI()));
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryWriteUsingOptions() throws IOException {
        final Table table = Table.read().file(PANDAS_PYARROW);
        table.write().usingOptions(TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table readTable = Table.read().file(OUTPUT_FILE);
        assertNotNull(readTable, "Read table is null");
    }

    @Test
    void testRegistryWriteFilename() throws IOException {
        final Table table = Table.read().file(PANDAS_PYARROW);
        final DataFrameWriter dfw = table.write();
        assertThrows(NullPointerException.class, () -> dfw.toFile(OUTPUT_FILE));
    }

    @Test
    void testRegistryWriteFile() throws IOException {
        final Table table = Table.read().file(PANDAS_PYARROW);
        final DataFrameWriter dfw = table.write();
        final File file = new File(OUTPUT_FILE);
        assertThrows(NullPointerException.class, () -> dfw.toFile(file));
    }
}
