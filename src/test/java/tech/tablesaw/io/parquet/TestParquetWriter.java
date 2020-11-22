package tech.tablesaw.io.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.Table;
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
    assertEquals(
        actual.rowCount(), expected.rowCount(), header + " tables should have same number of rows");
    assertEquals(
        actual.columnCount(),
        expected.columnCount(),
        header + " tables should have same number of columns");
    final int maxRows = actual.rowCount();
    final int numberOfColumns = actual.columnCount();
    for (int rowIndex = 0; rowIndex < maxRows; rowIndex++) {
      for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
        assertEquals(
            actual.get(rowIndex, columnIndex),
            expected.get(rowIndex, columnIndex),
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
}
