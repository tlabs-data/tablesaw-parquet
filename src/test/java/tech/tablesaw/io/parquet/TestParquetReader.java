package tech.tablesaw.io.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.parquet.TablesawParquetReadOptions.Builder;
import tech.tablesaw.io.parquet.TablesawParquetReadOptions.ManageGroupsAs;

class TestParquetReader {

  private static final String APACHE_ALL_TYPES_DICT =
      "src/test/resources/alltypes_dictionary.parquet";
  private static final String APACHE_ALL_TYPES_PLAIN = "src/test/resources/alltypes_plain.parquet";
  private static final String APACHE_ALL_TYPES_SNAPPY =
      "src/test/resources/alltypes_plain.snappy.parquet";
  private static final String APACHE_BINARY = "src/test/resources/binary.parquet";
  private static final String APACHE_DATAPAGEV2 = "src/test/resources/datapage_v2.snappy.parquet";
  private static final String APACHE_DICT_PAGE_OFFSET0 =
      "src/test/resources/dict-page-offset-zero.parquet";
  private static final String APACHE_INT32_DECIMAL = "src/test/resources/int32_decimal.parquet";
  private static final String APACHE_INT64_DECIMAL = "src/test/resources/int64_decimal.parquet";
  private static final String APACHE_FIXED_LENGTH_DECIMAL =
      "src/test/resources/fixed_length_decimal.parquet";
  private static final String APACHE_FIXED_LENGTH_DECIMAL_LEGACY =
      "src/test/resources/fixed_length_decimal_legacy.parquet";
  private static final String APACHE_BYTE_ARRAY_DECIMAL =
      "src/test/resources/byte_array_decimal.parquet";
  private static final String APACHE_NATION_MALFORMED =
      "src/test/resources/nation.dict-malformed.parquet";
  private static final String APACHE_NULLS_SNAPPY = "src/test/resources/nulls.snappy.parquet";

  private void validateTable(final Table table, int cols, int rows, String source) {
    assertNotNull(table, source + " is null");
    assertEquals(cols, table.columnCount(), source + " wrong column count");
    assertEquals(rows, table.rowCount(), source + " wrong row count");
  }

  @Test
  void testTableNameFromFile() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(new File(APACHE_ALL_TYPES_DICT)).build());
    assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
    assertEquals(
        "alltypes_dictionary.parquet", table.name(), APACHE_ALL_TYPES_DICT + " wrong name");
  }

  @Test
  void testTableNameFromOptions() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(new File(APACHE_ALL_TYPES_DICT))
                    .tableName("ANOTHERNAME")
                    .build());
    assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
    assertEquals("ANOTHERNAME", table.name(), APACHE_ALL_TYPES_DICT + " wrong name");
  }

  @Test
  void testTableFromURL() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(new File(APACHE_ALL_TYPES_DICT).toURI().toURL())
                    .build());
    assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
    assertTrue(table.name().startsWith("file:/"));
  }

  @Test
  void testColumnNames() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT).build());
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
    validateAllTypesDefault(
        APACHE_ALL_TYPES_DICT, TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT), 2);
  }

  @Test
  void testAllTypesPlain() throws IOException {
    validateAllTypesDefault(
        APACHE_ALL_TYPES_PLAIN, TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN), 8);
  }

  @Test
  void testAllTypesSnappy() throws IOException {
    validateAllTypesDefault(
        APACHE_ALL_TYPES_SNAPPY, TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_SNAPPY), 2);
  }

  private Table validateAllTypesDefault(
      final String sourceName, final Builder builder, final int rows) throws IOException {
    final Table table = new TablesawParquetReader().read(builder.build());
    validateTable(table, 11, rows, sourceName);
    assertEquals(
        ColumnType.INTEGER, table.column(0).type(), sourceName + "[" + "id" + "] wrong type");
    assertEquals(
        ColumnType.BOOLEAN, table.column(1).type(), sourceName + "[" + "bool_col" + "] wrong type");
    assertEquals(
        ColumnType.INTEGER,
        table.column(2).type(),
        sourceName + "[" + "tinyint_col" + "] wrong type");
    assertEquals(
        ColumnType.INTEGER,
        table.column(3).type(),
        sourceName + "[" + "smallint_col" + "] wrong type");
    assertEquals(
        ColumnType.INTEGER, table.column(4).type(), sourceName + "[" + "int_col" + "] wrong type");
    assertEquals(
        ColumnType.LONG, table.column(5).type(), sourceName + "[" + "bigint_col" + "] wrong type");
    assertEquals(
        ColumnType.FLOAT, table.column(6).type(), sourceName + "[" + "float_col" + "] wrong type");
    assertEquals(
        ColumnType.DOUBLE,
        table.column(7).type(),
        sourceName + "[" + "double_col" + "] wrong type");
    assertEquals(
        ColumnType.STRING,
        table.column(8).type(),
        sourceName + "[" + "date_string_col" + "] wrong type");
    assertEquals(
        ColumnType.STRING,
        table.column(9).type(),
        sourceName + "[" + "string_col" + "] wrong type");
    assertEquals(
        ColumnType.STRING,
        table.column(10).type(),
        sourceName + "[" + "timestamp_col" + "] wrong type");
    return table;
  }

  @Test
  void testInt96AsTimestamp() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN)
                    .withConvertInt96ToTimestamp(true)
                    .build());
    validateTable(table, 11, 8, APACHE_ALL_TYPES_PLAIN);
    assertEquals(
        ColumnType.INSTANT,
        table.column(10).type(),
        APACHE_ALL_TYPES_PLAIN + "[" + "timestamp_col" + "] wrong type");
  }

  @Test
  void testBinaryUTF8() throws IOException {
    final Table table =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(APACHE_BINARY).build());
    validateTable(table, 1, 12, APACHE_BINARY);
    assertEquals(
        ColumnType.STRING, table.column(0).type(), APACHE_BINARY + "[" + "foo" + "] wrong type");
    assertEquals(
        StandardCharsets.UTF_8.decode(ByteBuffer.wrap(new byte[] {0})).toString(),
        table.getString(0, 0),
        APACHE_BINARY + "[" + "foo" + ",0] wrong value");
    assertEquals(
        StandardCharsets.UTF_8.decode(ByteBuffer.wrap(new byte[] {1})).toString(),
        table.getString(1, 0),
        APACHE_BINARY + "[" + "foo" + ",1] wrong value");
  }

  @Test
  void testBinaryRaw() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(APACHE_BINARY)
                    .withUnnanotatedBinaryAsString(false)
                    .build());
    validateTable(table, 1, 12, APACHE_BINARY);
    assertEquals(
        ColumnType.STRING, table.column(0).type(), APACHE_BINARY + "[" + "foo" + "] wrong type");
    assertEquals("00", table.getString(0, 0), APACHE_BINARY + "[" + "foo" + ",0] wrong value");
    assertEquals("01", table.getString(1, 0), APACHE_BINARY + "[" + "foo" + ",0] wrong value");
  }

  @Test
  void testDataPageV2() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2).build());
    validateTable(table, 5, 5, APACHE_DATAPAGEV2);
    assertEquals(
        ColumnType.STRING, table.column(0).type(), APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
    assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
    assertEquals(
        ColumnType.INTEGER, table.column(1).type(), APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
    assertEquals(
        ColumnType.DOUBLE, table.column(2).type(), APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
    assertEquals(
        ColumnType.BOOLEAN, table.column(3).type(), APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
    assertEquals(
        ColumnType.TEXT, table.column(4).type(), APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
  }

  @Test
  void testDataPageV2RawBinary() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2)
                    .withUnnanotatedBinaryAsString(false)
                    .build());
    validateTable(table, 5, 5, APACHE_DATAPAGEV2);
    assertEquals(
        ColumnType.STRING, table.column(0).type(), APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
    assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
    assertEquals(
        ColumnType.INTEGER, table.column(1).type(), APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
    assertEquals(
        ColumnType.DOUBLE, table.column(2).type(), APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
    assertEquals(
        ColumnType.BOOLEAN, table.column(3).type(), APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
    assertEquals(
        ColumnType.TEXT, table.column(4).type(), APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
  }

  @Test
  void testDataPageV2Text() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2)
                    .withManageGroupAs(ManageGroupsAs.TEXT)
                    .build());
    validateTable(table, 5, 5, APACHE_DATAPAGEV2);
    assertEquals(
        ColumnType.STRING, table.column(0).type(), APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
    assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
    assertEquals(
        ColumnType.INTEGER, table.column(1).type(), APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
    assertEquals(
        ColumnType.DOUBLE, table.column(2).type(), APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
    assertEquals(
        ColumnType.BOOLEAN, table.column(3).type(), APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
    assertEquals(
        ColumnType.TEXT, table.column(4).type(), APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
  }

  @Test
  void testDataPageV2Skip() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2)
                    .withManageGroupAs(ManageGroupsAs.SKIP)
                    .build());
    validateTable(table, 4, 5, APACHE_DATAPAGEV2);
    assertEquals(
        ColumnType.STRING, table.column(0).type(), APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
    assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
    assertEquals(
        ColumnType.INTEGER, table.column(1).type(), APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
    assertEquals(
        ColumnType.DOUBLE, table.column(2).type(), APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
    assertEquals(
        ColumnType.BOOLEAN, table.column(3).type(), APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
  }

  @Test()
  void testDataPageV2Error() throws IOException {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            new TablesawParquetReader()
                .read(
                    TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2)
                        .withManageGroupAs(ManageGroupsAs.ERROR)
                        .build()));
  }

  @Test
  void testDictPageOffset0() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_DICT_PAGE_OFFSET0).build());
    validateTable(table, 1, 39, APACHE_DICT_PAGE_OFFSET0);
    assertEquals(
        ColumnType.INTEGER,
        table.column(0).type(),
        APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + "] wrong type");
    assertEquals(
        1552,
        table.intColumn(0).getInt(0),
        APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + ",0] wrong value");
    assertEquals(
        1552,
        table.intColumn(0).getInt(38),
        APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + ",38] wrong value");
  }

  @Test
  void testInt32Decimal() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_INT32_DECIMAL).build());
    validateTable(table, 1, 24, APACHE_INT32_DECIMAL);
    assertEquals(
        ColumnType.INTEGER,
        table.column(0).type(),
        APACHE_INT32_DECIMAL + "[" + "value" + "] wrong type");
    assertEquals(
        100,
        table.intColumn(0).getInt(0),
        APACHE_INT32_DECIMAL + "[" + "value" + ",0] wrong value");
    assertEquals(
        2400,
        table.intColumn(0).getInt(23),
        APACHE_INT32_DECIMAL + "[" + "value" + ",23] wrong value");
  }

  @Test
  void testInt64Decimal() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_INT64_DECIMAL).build());
    validateTable(table, 1, 24, APACHE_INT64_DECIMAL);
    assertEquals(
        ColumnType.LONG,
        table.column(0).type(),
        APACHE_INT64_DECIMAL + "[" + "value" + "] wrong type");
    assertEquals(
        100l,
        table.longColumn(0).getLong(0),
        APACHE_INT64_DECIMAL + "[" + "value" + ",0] wrong value");
    assertEquals(
        2400l,
        table.longColumn(0).getLong(23),
        APACHE_INT64_DECIMAL + "[" + "value" + ",23] wrong value");
  }

  @Test
  void testFixedLengthDecimal() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_FIXED_LENGTH_DECIMAL).build());
    validateTable(table, 1, 24, APACHE_FIXED_LENGTH_DECIMAL);
    assertEquals(
        ColumnType.DOUBLE,
        table.column(0).type(),
        APACHE_FIXED_LENGTH_DECIMAL + "[" + "value" + "] wrong type");
    assertEquals(
        1.0d,
        table.doubleColumn(0).getDouble(0),
        APACHE_FIXED_LENGTH_DECIMAL + "[" + "value" + ",0] wrong value");
    assertEquals(
        24.0d,
        table.doubleColumn(0).getDouble(23),
        APACHE_FIXED_LENGTH_DECIMAL + "[" + "value" + "23] wrong value");
  }

  @Test
  void testFixedLengthDecimalLegacy() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_FIXED_LENGTH_DECIMAL_LEGACY).build());
    validateTable(table, 1, 24, APACHE_FIXED_LENGTH_DECIMAL_LEGACY);
    assertEquals(
        ColumnType.DOUBLE,
        table.column(0).type(),
        APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "value" + "] wrong type");
    assertEquals(
        1.0d,
        table.doubleColumn(0).getDouble(0),
        APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "value" + ",0] wrong value");
    assertEquals(
        24.0d,
        table.doubleColumn(0).getDouble(23),
        APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "value" + ",23] wrong value");
  }

  @Test
  void testByteArrayDecimal() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_BYTE_ARRAY_DECIMAL).build());
    validateTable(table, 1, 24, APACHE_BYTE_ARRAY_DECIMAL);
    assertEquals(
        ColumnType.DOUBLE,
        table.column(0).type(),
        APACHE_BYTE_ARRAY_DECIMAL + "[" + "value" + "] wrong type");
    assertEquals(
        1.0d,
        table.doubleColumn(0).getDouble(0),
        APACHE_BYTE_ARRAY_DECIMAL + "[" + "value" + ",0] wrong value");
    assertEquals(
        24.0d,
        table.doubleColumn(0).getDouble(23),
        APACHE_BYTE_ARRAY_DECIMAL + "[" + "value" + ",23] wrong value");
  }

  @Test
  @Disabled("EOFException with this data file")
  void testNationMalformed() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_NATION_MALFORMED).build());
    validateTable(table, 4, 25, APACHE_NATION_MALFORMED);
    assertEquals(
        table.column(0).type(),
        ColumnType.INTEGER,
        APACHE_NATION_MALFORMED + "[" + "nation_key" + "] wrong type");
    assertEquals(
        table.column(1).type(),
        ColumnType.STRING,
        APACHE_NATION_MALFORMED + "[" + "name" + "] wrong type");
    assertEquals(
        table.column(2).type(),
        ColumnType.INTEGER,
        APACHE_NATION_MALFORMED + "[" + "region_key" + "] wrong type");
    assertEquals(
        table.column(3).type(),
        ColumnType.STRING,
        APACHE_NATION_MALFORMED + "[" + "comment_col" + "] wrong type");
  }

  @Test
  void testNullsSnappy() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_NULLS_SNAPPY).build());
    validateTable(table, 1, 8, APACHE_NULLS_SNAPPY);
    assertEquals(
        table.column(0).type(),
        ColumnType.TEXT,
        APACHE_NULLS_SNAPPY + "[" + "b_struct" + "] wrong type");
  }

  @Test
  void testNullsSnappySkip() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(APACHE_NULLS_SNAPPY)
                    .withManageGroupAs(ManageGroupsAs.SKIP)
                    .build());
    validateTable(table, 0, 0, APACHE_NULLS_SNAPPY);
  }
}
