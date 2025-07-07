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

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.parquet.io.api.Binary;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.Table;

class TestRowFiltering {

    private static final String PARQUET_TESTING_FOLDER = "target/test/data/parquet-testing-master/data/";
    private static final String APACHE_ALL_TYPES_PLAIN = "alltypes_plain.parquet";
    private static final String PANDAS_PYARROW = "target/test-classes/pandas_pyarrow.parquet";

    private static final TablesawParquetReader PARQUET_READER = new TablesawParquetReader();

    @Test
    void testBooleanFilter() {
        final TablesawParquetReadOptions options =
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN)
            .withRecordFilter(eq(booleanColumn("bool_col"), Boolean.TRUE))
            .build();
        final Table table = PARQUET_READER.read(options);
        assertEquals(4, table.rowCount());
    }

    @Test
    void testIntFilter() {
        final TablesawParquetReadOptions options =
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN)
            .withConvertInt96ToTimestamp(true)
            .withRecordFilter(gt(longColumn("bigint_col"), 5l))
            .build();
        final Table table = PARQUET_READER.read(options);
        assertEquals(4, table.rowCount());
    }

    @Test
    void testDoubleFilter() {
        final TablesawParquetReadOptions options =
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN)
            .withConvertInt96ToTimestamp(true)
            .withRecordFilter(ltEq(doubleColumn("double_col"), 5d))
            .build();
        final Table table = PARQUET_READER.read(options);
        assertEquals(4, table.rowCount());
    }

    @Test
    void testStringFilter() {
        final TablesawParquetReadOptions options =
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN)
            .withConvertInt96ToTimestamp(true)
            .withRecordFilter(gt(binaryColumn("date_string_col"), Binary.fromString("02/01/09")))
            .build();
        final Table table = PARQUET_READER.read(options);
        assertEquals(4, table.rowCount());
    }

    @Test
    void testDateTimeFilter() {
        final long dateTimeLongValue = LocalDateTime.parse("2021-04-23T00:00:00.000000")
            .toEpochSecond(ZoneOffset.UTC) * 1000; // MILLIS
        final TablesawParquetReadOptions options =
            TablesawParquetReadOptions.builder(PANDAS_PYARROW)
            .withConvertInt96ToTimestamp(true)
            .withRecordFilter(gt(longColumn("datetime"), dateTimeLongValue))
            .build();
        final Table table = PARQUET_READER.read(options);
        assertEquals(1, table.rowCount());
    }

    @Test
    void testCombinedFilter() {
        final TablesawParquetReadOptions options =
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN)
            .withConvertInt96ToTimestamp(true)
            .withRecordFilter(and(
                gt(binaryColumn("date_string_col"), Binary.fromString("02/01/09")),
                eq(booleanColumn("bool_col"), Boolean.TRUE)))
            .build();
        final Table table = PARQUET_READER.read(options);
        assertEquals(2, table.rowCount());
    }
}
