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

import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;

class TestColumnTypesOptions {

    private static final String PANDAS_PYARROW = "target/test-classes/pandas_pyarrow.parquet";

    @Test
    void testLocalDateTimeToInstant() {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(PANDAS_PYARROW)
                .columnTypesPartial(Collections.singletonMap("datetime", ColumnType.INSTANT))
                .build());
        assertEquals(13, table.columnCount(), "Wrong number of columns");
        assertEquals(ColumnType.INSTANT, table.column(11).type(), "Column type not set to Instant");
    }
    
    @Test
    void testColumnTypeSkipPartial() {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(PANDAS_PYARROW)
                .columnTypesPartial(name -> Optional.of(name)
                    .map(n -> n.startsWith("u") ? ColumnType.SKIP : null))
                .build());
        assertEquals(9, table.columnCount(), "Wrong number of columns");
    }
    
    @Test
    void testColumnTypeSkipFull() {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(PANDAS_PYARROW)
                .columnTypes(name -> {
                    if(name.contains("int")) return ColumnType.INTEGER;
                    if(name.contains("long")) return ColumnType.LONG;
                    return ColumnType.SKIP;
                })
                .build());
        assertEquals(4, table.columnCount(), "Wrong number of columns");
    }
    
    @Test
    void testColumnTypeWithSelectedColumns() {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(PANDAS_PYARROW)
                .withOnlyTheseColumns("byte", "ubyte")
                .columnTypes(new ColumnType[] {ColumnType.LONG, ColumnType.SHORT})
                .build());
        assertEquals(2, table.columnCount(), "Wrong number of columns");
        assertEquals("byte", table.column(0).name(), "Wrong Column name for byte column");
        assertEquals(ColumnType.LONG, table.column(0).type(), "Wrong ColumnType for byte column");
        assertEquals("ubyte", table.column(1).name(), "Wrong Column name for ubyte column");
        assertEquals(ColumnType.SHORT, table.column(1).type(), "Wrong ColumnType for ubyte column");
    }

    @Test
    void testPartialColumnTypeWithSelectedColumns() {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(PANDAS_PYARROW)
                .withOnlyTheseColumns("byte", "ubyte")
                .columnTypesPartial(Collections.singletonMap("ubyte", ColumnType.SHORT))
                .build());
        assertEquals(2, table.columnCount(), "Wrong number of columns");
        assertEquals("byte", table.column(0).name(), "Wrong Column name for byte column");
        assertEquals(ColumnType.INTEGER, table.column(0).type(), "Wrong ColumnType for byte column");
        assertEquals("ubyte", table.column(1).name(), "Wrong Column name for ubyte column");
        assertEquals(ColumnType.SHORT, table.column(1).type(), "Wrong ColumnType for ubyte column");
    }

}
