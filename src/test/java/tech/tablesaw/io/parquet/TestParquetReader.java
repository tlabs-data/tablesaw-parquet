package tech.tablesaw.io.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.parquet.TablesawParquetReadOptions.Builder;
import tech.tablesaw.io.parquet.TablesawParquetReadOptions.ManageGroupsAs;

class TestParquetReader {

	private static final String APACHE_ALL_TYPES_DICT = "src/test/resources/alltypes_dictionary.parquet";
	private static final String APACHE_ALL_TYPES_PLAIN = "src/test/resources/alltypes_plain.parquet";
	private static final String APACHE_ALL_TYPES_SNAPPY = "src/test/resources/alltypes_plain.snappy.parquet";
	private static final String APACHE_BINARY = "src/test/resources/binary.parquet";
	private static final String APACHE_DATAPAGEV2 = "src/test/resources/datapage_v2.snappy.parquet";
	private static final String APACHE_DICT_PAGE_OFFSET0 = "src/test/resources/dict-page-offset-zero.parquet";
	private static final String APACHE_INT32_DECIMAL = "src/test/resources/int32_decimal.parquet";
	private static final String APACHE_INT64_DECIMAL = "src/test/resources/int64_decimal.parquet";
	private static final String APACHE_FIXED_LENGTH_DECIMAL = "src/test/resources/fixed_length_decimal.parquet";
	private static final String APACHE_FIXED_LENGTH_DECIMAL_LEGACY = "src/test/resources/fixed_length_decimal_legacy.parquet";

	public static void printTable(final Table table, final String source) {
		System.out.println("Table " + source);
		System.out.println(table.structure());
		System.out.println(table.print(10));
	}
	
	private void validateTable(final Table table, int cols, int rows, String source) {
		assertNotNull(table, source + " is null");
		assertEquals(cols, table.columnCount(), source + " wrong column count");
		assertEquals(rows, table.rowCount(), source + " wrong row count");
	}
	
	@Test
	void testTableNameFromFile() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT).build());
		assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
		assertEquals("alltypes_dictionary.parquet", table.name(), APACHE_ALL_TYPES_DICT + " wrong name");
	}
	
	@Test
	void testTableNameFromOptions() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT)
					.tableName("ANOTHERNAME").build());
		assertNotNull(table, APACHE_ALL_TYPES_DICT + " null table");
		assertEquals("ANOTHERNAME", table.name(), APACHE_ALL_TYPES_DICT + " wrong name");
	}
	
	@Test
	void testColumnNames() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT).build());
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
		validateAllTypesDefault(APACHE_ALL_TYPES_DICT, TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT), 2);
	}

	@Test
	void testAllTypesPlain() throws IOException {
		validateAllTypesDefault(APACHE_ALL_TYPES_PLAIN, TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN), 8);
	}

	@Test
	void testAllTypesSnappy() throws IOException {
		validateAllTypesDefault(APACHE_ALL_TYPES_SNAPPY, TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_SNAPPY), 2);
	}

	private Table validateAllTypesDefault(final String sourceName, final Builder builder, final int rows) throws IOException {
		final Table table = new TablesawParquetReader().read(builder.build());
		validateTable(table, 11, rows, sourceName);
		assertTrue(table.column(0).type() == ColumnType.INTEGER, sourceName + "[" + "id" + "] wrong type");
		assertTrue(table.column(1).type() == ColumnType.BOOLEAN, sourceName + "[" + "bool_col" + "] wrong type");
		assertTrue(table.column(2).type() == ColumnType.INTEGER, sourceName + "[" + "tinyint_col" + "] wrong type");
		assertTrue(table.column(3).type() == ColumnType.INTEGER, sourceName + "[" + "smallint_col" + "] wrong type");
		assertTrue(table.column(4).type() == ColumnType.INTEGER, sourceName + "[" + "int_col" + "] wrong type");
		assertTrue(table.column(5).type() == ColumnType.LONG, sourceName + "[" + "bigint_col" + "] wrong type");
		assertTrue(table.column(6).type() == ColumnType.FLOAT, sourceName + "[" + "float_col" + "] wrong type");
		assertTrue(table.column(7).type() == ColumnType.DOUBLE, sourceName + "[" + "double_col" + "] wrong type");
		assertTrue(table.column(8).type() == ColumnType.STRING, sourceName + "[" + "date_string_col" + "] wrong type");
		assertTrue(table.column(9).type() == ColumnType.STRING, sourceName + "[" + "string_col" + "] wrong type");
		assertTrue(table.column(10).type() == ColumnType.STRING, sourceName + "[" + "timestamp_col" + "] wrong type");
		return table;
	}

	@Test
	void testInt96AsTimestamp() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN)
					.withConvertInt96ToTimestamp(true).build());
		validateTable(table, 11, 8, APACHE_ALL_TYPES_PLAIN);
		assertTrue(table.column(10).type() == ColumnType.INSTANT, APACHE_ALL_TYPES_PLAIN + "[" + "timestamp_col" + "] wrong type");
	}

	@Test
	void testBinaryUTF8() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_BINARY).build());
		validateTable(table, 1, 12, APACHE_BINARY);
		assertTrue(table.column(0).type() == ColumnType.STRING, APACHE_BINARY + "[" + "foo" + "] wrong type");
		assertEquals(StandardCharsets.UTF_8.decode(ByteBuffer.wrap(new byte[] {0})).toString(), table.getString(0, 0),
				APACHE_BINARY + "[" + "foo" + ",0] wrong value");
		assertEquals(StandardCharsets.UTF_8.decode(ByteBuffer.wrap(new byte[] {1})).toString(), table.getString(1, 0),
				APACHE_BINARY + "[" + "foo" + ",0] wrong value");
	}

	@Test
	void testBinaryRaw() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_BINARY)
				.withUnnanotatedBinaryAsString(false).build());
		validateTable(table, 1, 12, APACHE_BINARY);
		assertTrue(table.column(0).type() == ColumnType.STRING, APACHE_BINARY + "[" + "foo" + "] wrong type");
		assertEquals("00", table.getString(0, 0), APACHE_BINARY + "[" + "foo" + ",0] wrong value");
		assertEquals("01", table.getString(1, 0), APACHE_BINARY + "[" + "foo" + ",0] wrong value");
	}
	
	@Test
	void testDataPageV2() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2).build());
		validateTable(table, 5, 5, APACHE_DATAPAGEV2);
		assertTrue(table.column(0).type() == ColumnType.STRING, APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
		assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
		assertTrue(table.column(1).type() == ColumnType.INTEGER, APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
		assertTrue(table.column(2).type() == ColumnType.DOUBLE, APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
		assertTrue(table.column(3).type() == ColumnType.BOOLEAN, APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
		assertTrue(table.column(4).type() == ColumnType.TEXT, APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
	}

	@Test
	void testDataPageV2RawBinary() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2)
				.withUnnanotatedBinaryAsString(false).build());
		validateTable(table, 5, 5, APACHE_DATAPAGEV2);
		assertTrue(table.column(0).type() == ColumnType.STRING, APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
		assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
		assertTrue(table.column(1).type() == ColumnType.INTEGER, APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
		assertTrue(table.column(2).type() == ColumnType.DOUBLE, APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
		assertTrue(table.column(3).type() == ColumnType.BOOLEAN, APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
		assertTrue(table.column(4).type() == ColumnType.TEXT, APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
	}

	@Test
	void testDataPageV2Text() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2)
				.withManageGroupAs(ManageGroupsAs.TEXT).build());
		validateTable(table, 5, 5, APACHE_DATAPAGEV2);
		assertTrue(table.column(0).type() == ColumnType.STRING, APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
		assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
		assertTrue(table.column(1).type() == ColumnType.INTEGER, APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
		assertTrue(table.column(2).type() == ColumnType.DOUBLE, APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
		assertTrue(table.column(3).type() == ColumnType.BOOLEAN, APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
		assertTrue(table.column(4).type() == ColumnType.TEXT, APACHE_DATAPAGEV2 + "[" + "e" + "] wrong type");
	}

	@Test
	void testDataPageV2Skip() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2)
				.withManageGroupAs(ManageGroupsAs.SKIP).build());
		validateTable(table, 4, 5, APACHE_DATAPAGEV2);
		assertTrue(table.column(0).type() == ColumnType.STRING, APACHE_DATAPAGEV2 + "[" + "a" + "] wrong type");
		assertEquals("abc", table.getString(0, 0), APACHE_DATAPAGEV2 + "[" + "a" + ",0] wrong value");
		assertTrue(table.column(1).type() == ColumnType.INTEGER, APACHE_DATAPAGEV2 + "[" + "b" + "] wrong type");
		assertTrue(table.column(2).type() == ColumnType.DOUBLE, APACHE_DATAPAGEV2 + "[" + "c" + "] wrong type");
		assertTrue(table.column(3).type() == ColumnType.BOOLEAN, APACHE_DATAPAGEV2 + "[" + "d" + "] wrong type");
	}

	@Test()
	void testDataPageV2Error() throws IOException {
		assertThrows(UnsupportedOperationException.class, () ->
			new TablesawParquetReader().read(
					TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2)
					.withManageGroupAs(ManageGroupsAs.ERROR).build())
			);
	}
	
	@Test
	void testDictPageOffset0() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_DICT_PAGE_OFFSET0).build());
		validateTable(table, 1, 39, APACHE_DICT_PAGE_OFFSET0);
		assertTrue(table.column(0).type() == ColumnType.INTEGER, APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + "] wrong type");
		assertEquals(1552, table.intColumn(0).getInt(0), APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + ",0] wrong value");
		assertEquals(1552, table.intColumn(0).getInt(38), APACHE_DICT_PAGE_OFFSET0 + "[" + "l_partkey" + ",0] wrong value");		
	}
	
	@Test
	void testInt32Decimal() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_INT32_DECIMAL).build());
		validateTable(table, 1, 24, APACHE_INT32_DECIMAL);
		assertTrue(table.column(0).type() == ColumnType.INTEGER, APACHE_INT32_DECIMAL + "[" + "l_partkey" + "] wrong type");
		assertEquals(100, table.intColumn(0).getInt(0), APACHE_INT32_DECIMAL + "[" + "l_partkey" + ",0] wrong value");
		assertEquals(2400, table.intColumn(0).getInt(23), APACHE_INT32_DECIMAL + "[" + "l_partkey" + ",0] wrong value");		
	}
	
	@Test
	void testInt64Decimal() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_INT64_DECIMAL).build());
		validateTable(table, 1, 24, APACHE_INT64_DECIMAL);
		assertEquals(table.column(0).type(), ColumnType.LONG, APACHE_INT64_DECIMAL + "[" + "l_partkey" + "] wrong type");
		assertEquals(100l, table.longColumn(0).getLong(0), APACHE_INT64_DECIMAL + "[" + "l_partkey" + ",0] wrong value");
		assertEquals(2400l, table.longColumn(0).getLong(23), APACHE_INT64_DECIMAL + "[" + "l_partkey" + ",0] wrong value");		
	}
	
	@Test
	void testFixedLengthDecimal() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_FIXED_LENGTH_DECIMAL).build());
		validateTable(table, 1, 24, APACHE_FIXED_LENGTH_DECIMAL);
		assertEquals(table.column(0).type(), ColumnType.DOUBLE, APACHE_FIXED_LENGTH_DECIMAL + "[" + "l_partkey" + "] wrong type");
		assertEquals(1.0d, table.doubleColumn(0).getDouble(0), APACHE_FIXED_LENGTH_DECIMAL + "[" + "l_partkey" + ",0] wrong value");
		assertEquals(24.0d, table.doubleColumn(0).getDouble(23), APACHE_FIXED_LENGTH_DECIMAL + "[" + "l_partkey" + ",0] wrong value");		
	}
	
	@Test
	void testFixedLengthDecimalLegacy() throws IOException {
		final Table table = new TablesawParquetReader().read(
				TablesawParquetReadOptions.builder(APACHE_FIXED_LENGTH_DECIMAL_LEGACY).build());
		validateTable(table, 1, 24, APACHE_FIXED_LENGTH_DECIMAL_LEGACY);
		assertEquals(table.column(0).type(), ColumnType.DOUBLE, APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "l_partkey" + "] wrong type");
		assertEquals(1.0d, table.doubleColumn(0).getDouble(0), APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "l_partkey" + ",0] wrong value");
		assertEquals(24.0d, table.doubleColumn(0).getDouble(23), APACHE_FIXED_LENGTH_DECIMAL_LEGACY + "[" + "l_partkey" + ",0] wrong value");		
	}
}
