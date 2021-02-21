package tech.tablesaw.io.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.BooleanColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.ShortColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.api.TextColumn;
import tech.tablesaw.api.TimeColumn;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.Destination;
import tech.tablesaw.io.parquet.TablesawParquetWriteOptions.CompressionCodec;

class TestParquetWriter {

  private static final String APACHE_ALL_TYPES_DICT =
      "src/test/resources/alltypes_dictionary.parquet";
  private static final String APACHE_ALL_TYPES_PLAIN = "src/test/resources/alltypes_plain.parquet";
  private static final String APACHE_ALL_TYPES_SNAPPY =
      "src/test/resources/alltypes_plain.snappy.parquet";
  private static final String APACHE_FIXED_LENGTH_DECIMAL =
      "src/test/resources/fixed_length_decimal.parquet";
  private static final String APACHE_BYTE_ARRAY_DECIMAL =
      "src/test/resources/byte_array_decimal.parquet";
  private static final String APACHE_DATAPAGEV2 = "src/test/resources/datapage_v2.snappy.parquet";
  private static final String OUTPUT_FILE = "target/test/results/out.parquet";

  public static void assertTableEquals(
      final Table expected, final Table actual, final String header) {
    final int numberOfColumns = actual.columnCount();
    assertEquals(
        expected.columnCount(),
        numberOfColumns,
        header + " tables should have same number of columns");
    for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
      final Column<?> actualColumn = actual.column(columnIndex);
      final Column<?> expectedColumn = expected.column(columnIndex);
      assertEquals(expectedColumn.name(), actualColumn.name(), "Wrong column name");
      // No good way to distinguish between string and text after writing
      if (actualColumn instanceof StringColumn) {
        assertTrue(
            expectedColumn instanceof StringColumn || expectedColumn instanceof TextColumn,
            "Column transformed to StringColumns");
      } else {
        assertEquals(expectedColumn.type(), actualColumn.type(), "Column type different");
      }
    }
    final int numberOfRows = actual.rowCount();
    assertEquals(
        expected.rowCount(), numberOfRows, header + " tables should have same number of rows");
    for (int rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
      for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
        assertEquals(
            expected.get(rowIndex, columnIndex),
            actual.get(rowIndex, columnIndex),
            header + " cells[" + rowIndex + ", " + columnIndex + "] do not match");
      }
    }
  }

  @Test
  void testReadWriteAllTypeDict() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT).build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_DICT + " reloaded");
  }

  @Test
  void testReadWriteAllTypeDictMinimized() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT)
                    .minimizeColumnSizes()
                    .build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(OUTPUT_FILE).minimizeColumnSizes().build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_DICT + " reloaded");
  }

  @Test
  void testReadWriteAllTypeDictToFile() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_DICT).build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(new File(OUTPUT_FILE)).build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_DICT + " reloaded");
  }

  @Test
  void testReadWriteAllTypePlain() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN).build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " reloaded");
  }

  @Test
  void testReadWriteAllTypeSnappy() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_SNAPPY).build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_SNAPPY + " reloaded");
  }

  @Test
  void testInt96AsTimestamp() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN)
                    .withConvertInt96ToTimestamp(true)
                    .build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " with Int96 as Timestamp reloaded");
  }

  @Test
  void testFixedLengthDecimal() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_FIXED_LENGTH_DECIMAL).build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_FIXED_LENGTH_DECIMAL + " reloaded");
  }

  @Test
  void testBinaryDecimal() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_BYTE_ARRAY_DECIMAL).build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_BYTE_ARRAY_DECIMAL + " reloaded");
  }

  @Test
  void testDatapageV2() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_DATAPAGEV2).build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_DATAPAGEV2 + " reloaded");
  }

  @Test
  void testOverwriteOption() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN).build());
    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    assertThrows(
        FileAlreadyExistsException.class,
        () ->
            new TablesawParquetWriter()
                .write(
                    orig,
                    TablesawParquetWriteOptions.builder(OUTPUT_FILE).withOverwrite(false).build()));
  }

  @Test
  void testGZIPCompressor() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN).build());
    new TablesawParquetWriter()
        .write(
            orig,
            TablesawParquetWriteOptions.builder(OUTPUT_FILE)
                .withCompressionCode(CompressionCodec.GZIP)
                .build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " gzip reloaded");
  }

  @Test
  void testPLAINCompressor() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN).build());
    new TablesawParquetWriter()
        .write(
            orig,
            TablesawParquetWriteOptions.builder(OUTPUT_FILE)
                .withCompressionCode(CompressionCodec.UNCOMPRESSED)
                .build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " gzip reloaded");
  }

  @Test
  void testSNAPPYCompressor() throws IOException {
    final Table orig =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_PLAIN).build());
    new TablesawParquetWriter()
        .write(
            orig,
            TablesawParquetWriteOptions.builder(OUTPUT_FILE)
                .withCompressionCode(CompressionCodec.SNAPPY)
                .build());
    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " gzip reloaded");
  }

  @Test
  void testDestinationWriteException() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> new TablesawParquetWriter().write(null, (Destination) null),
        "Wrong exception on writing to destination");
  }

  @Test
  void testWriteReadAllColumnTypes() throws IOException {
    final Table orig =
        Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0l, 500_000_000_000l),
            FloatColumn.create("float", Float.NaN, 3.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d),
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
    final Table dest =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(OUTPUT_FILE).minimizeColumnSizes().build());
    assertTableEquals(orig, dest, "All ColumnTypes reloaded");
  }

  @Test
  void testWriteReadDefaultTypes() throws IOException {
    final Table orig =
        Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0l, 500_000_000_000l),
            FloatColumn.create("float", Float.NaN, 3.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d),
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());

    orig.replaceColumn("short", orig.shortColumn("short").asIntColumn().setName("short"));
    orig.replaceColumn("float", orig.floatColumn("float").asDoubleColumn().setName("float"));

    final Table dest =
        new TablesawParquetReader().read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
    assertTableEquals(orig, dest, "All ColumnTypes reloaded");
  }

  @Test
  void testWriteReadEmptyColumnTypeList() throws IOException {
    final Table orig =
        Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0l, 500_000_000_000l),
            FloatColumn.create("float", Float.NaN, 3.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d),
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());

    orig.replaceColumn("short", orig.shortColumn("short").asIntColumn().setName("short"));
    orig.replaceColumn("float", orig.floatColumn("float").asDoubleColumn().setName("float"));

    final Table dest =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(OUTPUT_FILE)
                    .columnTypesToDetect(new ArrayList<>())
                    .build());
    assertTableEquals(orig, dest, "All ColumnTypes reloaded");
  }

  @Test
  void testWriteReadNoShorts() throws IOException {
    final Table orig =
        Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0l, 500_000_000_000l),
            FloatColumn.create("float", Float.NaN, 3.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d),
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());

    orig.replaceColumn("short", orig.shortColumn("short").asIntColumn().setName("short"));

    final List<ColumnType> types = new ArrayList<>();
    types.add(ColumnType.FLOAT);
    final Table dest =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(OUTPUT_FILE).columnTypesToDetect(types).build());
    assertTableEquals(orig, dest, "All ColumnTypes reloaded");
  }

  @Test
  void testWriteReadNoFloat() throws IOException {
    final Table orig =
        Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0l, 500_000_000_000l),
            FloatColumn.create("float", Float.NaN, 3.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d),
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

    new TablesawParquetWriter()
        .write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());

    orig.replaceColumn("float", orig.floatColumn("float").asDoubleColumn().setName("float"));

    final List<ColumnType> types = new ArrayList<>();
    types.add(ColumnType.SHORT);
    final Table dest =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(OUTPUT_FILE).columnTypesToDetect(types).build());
    assertTableEquals(orig, dest, "All ColumnTypes reloaded");
  }
}
