package tech.tablesaw.io.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.parquet.TablesawParquetReadOptions.Builder;

class TestParquetReader {

	private static final String APACHE_ALL_TYPES_DICT = "src/test/resources/alltypes_dictionary.parquet";
	private static final String APACHE_ALL_TYPES_PLAIN = "src/test/resources/alltypes_plain.parquet";
	private static final String APACHE_ALL_TYPES_SNAPPY = "src/test/resources/alltypes_plain.snappy.parquet";

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
		final Table table = validateAllTypesDefault(APACHE_ALL_TYPES_PLAIN, TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN), 8);
		printTable(table, APACHE_ALL_TYPES_PLAIN);
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
		printTable(table, APACHE_ALL_TYPES_PLAIN);
	}


}
