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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions.Builder;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions.ManageGroupsAs;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions.UnnanotatedBinaryAs;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.Source;

class TestParquetReader {

    private static final String PARQUET_TESTING_FOLDER = "target/test/data/parquet-testing-master/data/";

    private static final String APACHE_ALL_TYPES_DICT = "alltypes_dictionary.parquet";
    private static final String APACHE_ALL_TYPES_PLAIN = "alltypes_plain.parquet";
    private static final String APACHE_ALL_TYPES_SNAPPY = "alltypes_plain.snappy.parquet";
    private static final String APACHE_BINARY = "binary.parquet";
    private static final String APACHE_DATAPAGEV2 = "datapage_v2.snappy.parquet";
    private static final String APACHE_DICT_PAGE_OFFSET0 = "dict-page-offset-zero.parquet";
    private static final String APACHE_INT32_DECIMAL = "int32_decimal.parquet";
    private static final String APACHE_INT64_DECIMAL = "int64_decimal.parquet";
    private static final String APACHE_FIXED_LENGTH_DECIMAL = "fixed_length_decimal.parquet";
    private static final String APACHE_FIXED_LENGTH_DECIMAL_LEGACY = "fixed_length_decimal_legacy.parquet";
    private static final String APACHE_BYTE_ARRAY_DECIMAL = "byte_array_decimal.parquet";
    private static final String APACHE_NULLS_SNAPPY = "nulls.snappy.parquet";

    private static final TablesawParquetReader PARQUET_READER = new TablesawParquetReader();

    private void validateTable(final Table table, int cols, int rows, String source) {
        assertNotNull(table, source + " is null");
        assertEquals(cols, table.columnCount(), source + " wrong column count");
        assertEquals(rows, table.rowCount(), source + " wrong row count");
    }

    @Test
    void testTableNameFromFile() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(new File(PARQUET_TESTING_FOLDER, APACHE_ALL_TYPES_DICT)).build());
        assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
        assertEquals("alltypes_dictionary.parquet", table.name(), APACHE_ALL_TYPES_DICT + " wrong name");
    }

    @Test
    void testTableNameFromOptions() throws IOException {
        final Table table = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(new File(PARQUET_TESTING_FOLDER, APACHE_ALL_TYPES_DICT)).tableName("ANOTHERNAME").build());
        assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
        assertEquals("ANOTHERNAME", table.name(), APACHE_ALL_TYPES_DICT + " wrong name");
    }

    @Test
    void testTableFromURL() throws IOException {
        final Table table = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(new File(PARQUET_TESTING_FOLDER, APACHE_ALL_TYPES_DICT).toURI().toURL()).build());
        assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
        assertTrue(table.name().startsWith("file:/"));
    }

    @Test
    void testTableFromFileSource() throws IOException {
        final Table table = PARQUET_READER.read(new Source(new File(PARQUET_TESTING_FOLDER, APACHE_ALL_TYPES_DICT)));
        assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
        assertEquals("alltypes_dictionary.parquet", table.name(), APACHE_ALL_TYPES_DICT + " wrong name");
    }

    @Test
    void testTableFromOtherSource() throws IOException {
        assertThrows(UnsupportedOperationException.class, () -> PARQUET_READER.read(Source.fromString("A,B,C,D,E")),
            "Wrong exception reading from non-file source");
    }

    @Test
    void testColumnNames() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_DICT).build());
        validateTable(table, 11, 2, APACHE_ALL_TYPES_DICT);
        assertEquals("id", table.column(0).name());
        assertEquals("bool_col", table.column(1).name());
        assertEquals("tinyint_col", table.column(2).name());
        assertEquals("smallint_col", table.column(3).name());
        assertEquals("int_col", table.column(4).name());
        assertEquals("bigint_col", table.column(5).name());
        assertEquals("float_col", table.column(6).name());
        assertEquals("double_col", table.column(7).name());
        assertEquals("date_string_col", table.column(8).name());
        assertEquals("string_col", table.column(9).name());
        assertEquals("timestamp_col", table.column(10).name());
    }

    @Test
    void testAllTypesDict() throws IOException {
        validateAllTypesDefault(APACHE_ALL_TYPES_DICT,
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_DICT).minimizeColumnSizes(),
            2);
    }

    @Test
    void testAllTypesPlain() throws IOException {
        validateAllTypesDefault(APACHE_ALL_TYPES_PLAIN,
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).minimizeColumnSizes(),
            8);
    }

    @Test
    void testAllTypesSnappy() throws IOException {
        validateAllTypesDefault(APACHE_ALL_TYPES_SNAPPY,
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_SNAPPY).minimizeColumnSizes(),
            2);
    }

    private void validateAllTypesDefault(final String sourceName, final Builder builder, final int rows)
        throws IOException {
        final Table table = PARQUET_READER.read(builder.build());
        validateTable(table, 11, rows, sourceName);
        assertEquals(ColumnType.INTEGER, table.column(0).type(), sourceName + "[" + "id" + "] wrong type");
        assertEquals(ColumnType.BOOLEAN, table.column(1).type(), sourceName + "[" + "bool_col" + "] wrong type");
        assertEquals(ColumnType.INTEGER, table.column(2).type(), sourceName + "[" + "tinyint_col" + "] wrong type");
        assertEquals(ColumnType.INTEGER, table.column(3).type(), sourceName + "[" + "smallint_col" + "] wrong type");
        assertEquals(ColumnType.INTEGER, table.column(4).type(), sourceName + "[" + "int_col" + "] wrong type");
        assertEquals(ColumnType.LONG, table.column(5).type(), sourceName + "[" + "bigint_col" + "] wrong type");
        assertEquals(ColumnType.FLOAT, table.column(6).type(), sourceName + "[" + "float_col" + "] wrong type");
        assertEquals(ColumnType.DOUBLE, table.column(7).type(), sourceName + "[" + "double_col" + "] wrong type");
        assertEquals(ColumnType.STRING, table.column(8).type(), sourceName + "[" + "date_string_col" + "] wrong type");
        assertEquals(ColumnType.STRING, table.column(9).type(), sourceName + "[" + "string_col" + "] wrong type");
        assertEquals(ColumnType.STRING, table.column(10).type(), sourceName + "[" + "timestamp_col" + "] wrong type");
    }

    @Test
    void testInt96AsTimestamp() throws IOException {
        final Table table = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).withConvertInt96ToTimestamp(true).build());
        validateTable(table, 11, 8, APACHE_ALL_TYPES_PLAIN);
        assertEquals(ColumnType.INSTANT, table.column(10).type(),
            APACHE_ALL_TYPES_PLAIN + "[" + "timestamp_col" + "] wrong type");
    }

    @Test
    void testBinaryUTF8() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_BINARY).build());
        validateTable(table, 1, 12, APACHE_BINARY);
        assertEquals(ColumnType.STRING, table.column(0).type(), APACHE_BINARY + "[" + "foo" + "] wrong type");
        assertEquals(StandardCharsets.UTF_8.decode(ByteBuffer.wrap(new byte[] { 0 })).toString(), table.getString(0, 0),
            APACHE_BINARY + "[" + "foo" + ",0] wrong value");
        assertEquals(StandardCharsets.UTF_8.decode(ByteBuffer.wrap(new byte[] { 1 })).toString(), table.getString(1, 0),
            APACHE_BINARY + "[" + "foo" + ",1] wrong value");
    }

    @Test
    void testBinaryRaw() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_BINARY)
                .withUnnanotatedBinaryAs(UnnanotatedBinaryAs.HEXSTRING).build());
        validateTable(table, 1, 12, APACHE_BINARY);
        assertEquals(ColumnType.STRING, table.column(0).type(), APACHE_BINARY + "[" + "foo" + "] wrong type");
        assertEquals("00", table.getString(0, 0), APACHE_BINARY + "[" + "foo" + ",0] wrong value");
        assertEquals("01", table.getString(1, 0), APACHE_BINARY + "[" + "foo" + ",0] wrong value");
    }

    @Test
    void testBinarySkip() throws IOException {
        final Table table = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(PARQUET_TESTING_FOLDER + APACHE_BINARY).withUnnanotatedBinaryAs(UnnanotatedBinaryAs.SKIP).build());
        assertEquals(0, table.columnCount(), "Unnanotated binary column should have beend skipped");
    }

    @Test
    void testDataPageV2() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_DATAPAGEV2).build());
        validateTable(table, 5, 5, APACHE_DATAPAGEV2);
        assertEquals(ColumnType.STRING, table.column(0).type(), APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
        assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
        assertEquals(ColumnType.INTEGER, table.column(1).type(), APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
        assertEquals(ColumnType.DOUBLE, table.column(2).type(), APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
        assertEquals(ColumnType.BOOLEAN, table.column(3).type(), APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
        assertEquals(ColumnType.TEXT, table.column(4).type(), APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
    }

    @Test
    void testDataPageV2RawBinary() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_DATAPAGEV2)
                .withUnnanotatedBinaryAs(UnnanotatedBinaryAs.HEXSTRING).build());
        validateTable(table, 5, 5, APACHE_DATAPAGEV2);
        assertEquals(ColumnType.STRING, table.column(0).type(), APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
        assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
        assertEquals(ColumnType.INTEGER, table.column(1).type(), APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
        assertEquals(ColumnType.DOUBLE, table.column(2).type(), APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
        assertEquals(ColumnType.BOOLEAN, table.column(3).type(), APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
        assertEquals(ColumnType.TEXT, table.column(4).type(), APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
    }

    @Test
    void testDataPageV2Text() throws IOException {
        final Table table = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(PARQUET_TESTING_FOLDER + APACHE_DATAPAGEV2).withManageGroupAs(ManageGroupsAs.TEXT).build());
        validateTable(table, 5, 5, APACHE_DATAPAGEV2);
        assertEquals(ColumnType.STRING, table.column(0).type(), APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
        assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
        assertEquals(ColumnType.INTEGER, table.column(1).type(), APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
        assertEquals(ColumnType.DOUBLE, table.column(2).type(), APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
        assertEquals(ColumnType.BOOLEAN, table.column(3).type(), APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
        assertEquals(ColumnType.TEXT, table.column(4).type(), APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
    }

    @Test
    void testDataPageV2Skip() throws IOException {
        final Table table = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(PARQUET_TESTING_FOLDER + APACHE_DATAPAGEV2).withManageGroupAs(ManageGroupsAs.SKIP).build());
        validateTable(table, 4, 5, APACHE_DATAPAGEV2);
        assertEquals(ColumnType.STRING, table.column(0).type(), APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
        assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
        assertEquals(ColumnType.INTEGER, table.column(1).type(), APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
        assertEquals(ColumnType.DOUBLE, table.column(2).type(), APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
        assertEquals(ColumnType.BOOLEAN, table.column(3).type(), APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
    }

    @Test()
    void testDataPageV2Error() throws IOException {
        assertThrows(UnsupportedOperationException.class, () -> PARQUET_READER.read(TablesawParquetReadOptions
            .builder(PARQUET_TESTING_FOLDER + APACHE_DATAPAGEV2).withManageGroupAs(ManageGroupsAs.ERROR).build()));
    }

    @Test
    void testDictPageOffset0() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_DICT_PAGE_OFFSET0).build());
        validateTable(table, 1, 39, APACHE_DICT_PAGE_OFFSET0);
        assertEquals(ColumnType.INTEGER, table.column(0).type(),
            APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + "] wrong type");
        assertEquals(1552, table.intColumn(0).getInt(0),
            APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + ",0] wrong value");
        assertEquals(1552, table.intColumn(0).getInt(38),
            APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + ",38] wrong value");
    }

    @Test
    void testInt32Decimal() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_INT32_DECIMAL).build());
        validateTable(table, 1, 24, APACHE_INT32_DECIMAL);
        assertEquals(ColumnType.INTEGER, table.column(0).type(), APACHE_INT32_DECIMAL + "[" + "value" + "] wrong type");
        assertEquals(100, table.intColumn(0).getInt(0), APACHE_INT32_DECIMAL + "[" + "value" + ",0] wrong value");
        assertEquals(2400, table.intColumn(0).getInt(23), APACHE_INT32_DECIMAL + "[" + "value" + ",23] wrong value");
    }

    @Test
    void testInt64Decimal() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_INT64_DECIMAL).build());
        validateTable(table, 1, 24, APACHE_INT64_DECIMAL);
        assertEquals(ColumnType.LONG, table.column(0).type(), APACHE_INT64_DECIMAL + "[" + "value" + "] wrong type");
        assertEquals(100l, table.longColumn(0).getLong(0), APACHE_INT64_DECIMAL + "[" + "value" + ",0] wrong value");
        assertEquals(2400l, table.longColumn(0).getLong(23), APACHE_INT64_DECIMAL + "[" + "value" + ",23] wrong value");
    }

    @Test
    void testFixedLengthDecimal() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_FIXED_LENGTH_DECIMAL).build());
        validateTable(table, 1, 24, APACHE_FIXED_LENGTH_DECIMAL);
        assertEquals(ColumnType.DOUBLE, table.column(0).type(),
            APACHE_FIXED_LENGTH_DECIMAL + "[" + "value" + "] wrong type");
        assertEquals(1.0d, table.doubleColumn(0).getDouble(0),
            APACHE_FIXED_LENGTH_DECIMAL + "[" + "value" + ",0] wrong value");
        assertEquals(24.0d, table.doubleColumn(0).getDouble(23),
            APACHE_FIXED_LENGTH_DECIMAL + "[" + "value" + "23] wrong value");
    }

    @Test
    void testFixedLengthDecimalLegacy() throws IOException {
        final Table table = PARQUET_READER.read(
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_FIXED_LENGTH_DECIMAL_LEGACY).build());
        validateTable(table, 1, 24, APACHE_FIXED_LENGTH_DECIMAL_LEGACY);
        assertEquals(ColumnType.DOUBLE, table.column(0).type(),
            APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "value" + "] wrong type");
        assertEquals(1.0d, table.doubleColumn(0).getDouble(0),
            APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "value" + ",0] wrong value");
        assertEquals(24.0d, table.doubleColumn(0).getDouble(23),
            APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "value" + ",23] wrong value");
    }

    @Test
    void testByteArrayDecimal() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_BYTE_ARRAY_DECIMAL).build());
        validateTable(table, 1, 24, APACHE_BYTE_ARRAY_DECIMAL);
        assertEquals(ColumnType.DOUBLE, table.column(0).type(),
            APACHE_BYTE_ARRAY_DECIMAL + "[" + "value" + "] wrong type");
        assertEquals(1.0d, table.doubleColumn(0).getDouble(0),
            APACHE_BYTE_ARRAY_DECIMAL + "[" + "value" + ",0] wrong value");
        assertEquals(24.0d, table.doubleColumn(0).getDouble(23),
            APACHE_BYTE_ARRAY_DECIMAL + "[" + "value" + ",23] wrong value");
    }

    @Test
    void testNullsSnappy() throws IOException {
        final Table table = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_NULLS_SNAPPY).build());
        validateTable(table, 1, 8, APACHE_NULLS_SNAPPY);
        assertEquals(ColumnType.TEXT, table.column(0).type(), APACHE_NULLS_SNAPPY + "[" + "b_struct" + "] wrong type");
    }

    @Test
    void testNullsSnappySkip() throws IOException {
        final Table table = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(PARQUET_TESTING_FOLDER + APACHE_NULLS_SNAPPY).withManageGroupAs(ManageGroupsAs.SKIP).build());
        validateTable(table, 0, 0, APACHE_NULLS_SNAPPY);
    }
}
