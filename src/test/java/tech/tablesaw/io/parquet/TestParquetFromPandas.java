package tech.tablesaw.io.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;

class TestParquetFromPandas {

  private static final String PANDAS_PYARROW = "target/test-classes/pandas_pyarrow.parquet";
  private static final String PANDAS_FASTPARQUET = "target/test-classes/pandas_fastparquet.parquet";

  @Test
  void testDefaultReadFromPyarrow() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(new File(PANDAS_PYARROW)).build());
    assertEquals("pandas_pyarrow.parquet", table.name(), "Wrong name from pyarrow");
    assertEquals(
        ColumnType.BOOLEAN, table.column(0).type(), "Wrong boolean column type from pyarrow");
    assertEquals(true, table.booleanColumn(0).get(0), "Wrong boolean value [0,0]");
    assertEquals(false, table.booleanColumn(0).get(1), "Wrong boolean value [0,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(1).type(), "Wrong uint8 column type from pyarrow");
    assertEquals(0, table.intColumn(1).get(0), "Wrong integer value [1,0]");
    assertEquals(127, table.intColumn(1).get(1), "Wrong integer value [1,1]");
    assertEquals(ColumnType.INTEGER, table.column(2).type(), "Wrong int8 column type from pyarrow");
    assertEquals(-127, table.intColumn(2).get(0), "Wrong integer value [2,0]");
    assertEquals(1, table.intColumn(2).get(1), "Wrong integer value [2,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(3).type(), "Wrong uint16 column type from pyarrow");
    assertEquals(0, table.intColumn(3).get(0), "Wrong integer value [3,0]");
    assertEquals(32767, table.intColumn(3).get(1), "Wrong integer value [3,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(4).type(), "Wrong int16 column type from pyarrow");
    assertEquals(0, table.intColumn(4).get(0), "Wrong integer value [4,0]");
    assertEquals(-32767, table.intColumn(4).get(1), "Wrong integer value [4,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(5).type(), "Wrong uint32 column type from pyarrow");
    assertEquals(0, table.intColumn(5).get(0), "Wrong integer value [5,0]");
    assertEquals(65000, table.intColumn(5).get(1), "Wrong integer value [5,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(6).type(), "Wrong int32 column type from pyarrow");
    assertEquals(0, table.intColumn(6).get(0), "Wrong integer value [6,0]");
    assertEquals(-65000, table.intColumn(6).get(1), "Wrong integer value [6,1]");
    assertEquals(ColumnType.LONG, table.column(7).type(), "Wrong uint64 column type from pyarrow");
    assertEquals(0, table.longColumn(7).get(0), "Wrong long value [7,0]");
    assertEquals(1_000_000_000, table.longColumn(7).get(1), "Wrong long value [7,1]");
    assertEquals(ColumnType.LONG, table.column(8).type(), "Wrong int64 column type from pyarrow");
    assertEquals(0, table.longColumn(8).get(0), "Wrong long value [8,0]");
    assertEquals(-1_000_000_000, table.longColumn(8).get(1), "Wrong long value [8,1]");
    assertEquals(
        ColumnType.DOUBLE, table.column(9).type(), "Wrong float32 column type from pyarrow");
    assertEquals(null, table.doubleColumn(9).get(0), "Wrong double value [9,0]");
    assertEquals(1.0d, table.doubleColumn(9).get(1), "Wrong double value [9,1]");
    assertEquals(
        ColumnType.DOUBLE, table.column(10).type(), "Wrong float64 column type from pyarrow");
    assertEquals(0.0d, table.doubleColumn(10).get(0), "Wrong double value [10,0]");
    assertEquals(null, table.doubleColumn(10).get(1), "Wrong double value [10,1]");
    assertEquals(
        ColumnType.LOCAL_DATE_TIME,
        table.column(11).type(),
        "Wrong TIMESTAMP_MILLIS column type from pyarrow");
    assertEquals(
        LocalDateTime.of(2021, 04, 23, 0, 0),
        table.dateTimeColumn(11).get(0),
        "Wrong datetime value [11,0]");
    assertEquals(
        LocalDateTime.of(2021, 04, 23, 0, 0, 1),
        table.dateTimeColumn(11).get(1),
        "Wrong datetime value [11,1]");
    assertEquals(
        ColumnType.STRING, table.column(12).type(), "Wrong string column type from pyarrow");
    assertEquals("string1", table.stringColumn(12).get(0), "Wrong string value [12,0]");
    assertEquals("string2", table.stringColumn(12).get(1), "Wrong string value [12,1]");
  }

  @Test
  void testMinimizedReadFromPyarrow() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(new File(PANDAS_PYARROW))
                    .minimizeColumnSizes()
                    .build());
    assertEquals("pandas_pyarrow.parquet", table.name(), "Wrong name from pyarrow");
    assertEquals(
        ColumnType.BOOLEAN, table.column(0).type(), "Wrong boolean column type from pyarrow");
    assertEquals(true, table.booleanColumn(0).get(0), "Wrong boolean value [0,0]");
    assertEquals(false, table.booleanColumn(0).get(1), "Wrong boolean value [0,1]");
    assertEquals(ColumnType.SHORT, table.column(1).type(), "Wrong uint8 column type from pyarrow");
    assertEquals((short) 0, table.shortColumn(1).get(0), "Wrong short value [1,0]");
    assertEquals((short) 127, table.shortColumn(1).get(1), "Wrong short value [1,1]");
    assertEquals(ColumnType.SHORT, table.column(2).type(), "Wrong int8 column type from pyarrow");
    assertEquals((short) -127, table.shortColumn(2).get(0), "Wrong short value [2,0]");
    assertEquals((short) 1, table.shortColumn(2).get(1), "Wrong short value [2,1]");
    assertEquals(ColumnType.SHORT, table.column(3).type(), "Wrong uint16 column type from pyarrow");
    assertEquals((short) 0, table.shortColumn(3).get(0), "Wrong short value [3,0]");
    assertEquals((short) 32767, table.shortColumn(3).get(1), "Wrong short value [3,1]");
    assertEquals(ColumnType.SHORT, table.column(4).type(), "Wrong int16 column type from pyarrow");
    assertEquals((short) 0, table.shortColumn(4).get(0), "Wrong short value [4,0]");
    assertEquals((short) -32767, table.shortColumn(4).get(1), "Wrong short value [4,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(5).type(), "Wrong uint32 column type from pyarrow");
    assertEquals(0, table.intColumn(5).get(0), "Wrong integer value [5,0]");
    assertEquals(65000, table.intColumn(5).get(1), "Wrong integer value [5,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(6).type(), "Wrong int32 column type from pyarrow");
    assertEquals(0, table.intColumn(6).get(0), "Wrong integer value [6,0]");
    assertEquals(-65000, table.intColumn(6).get(1), "Wrong integer value [6,1]");
    assertEquals(ColumnType.LONG, table.column(7).type(), "Wrong uint64 column type from pyarrow");
    assertEquals(0, table.longColumn(7).get(0), "Wrong long value [7,0]");
    assertEquals(1_000_000_000, table.longColumn(7).get(1), "Wrong long value [7,1]");
    assertEquals(ColumnType.LONG, table.column(8).type(), "Wrong int164 column type from pyarrow");
    assertEquals(0, table.longColumn(8).get(0), "Wrong long value [8,0]");
    assertEquals(-1_000_000_000, table.longColumn(8).get(1), "Wrong long value [8,1]");
    assertEquals(ColumnType.FLOAT, table.column(9).type(), "Wrong float32 from pyarrow");
    assertEquals(null, table.floatColumn(9).get(0), "Wrong float value [9,0]");
    assertEquals(1.0f, table.floatColumn(9).get(1), "Wrong float value [9,1]");
    assertEquals(ColumnType.DOUBLE, table.column(10).type(), "Wrong float64 from pyarrow");
    assertEquals(0.0d, table.doubleColumn(10).get(0), "Wrong double value [10,0]");
    assertEquals(null, table.doubleColumn(10).get(1), "Wrong double value [10,1]");
    assertEquals(
        ColumnType.LOCAL_DATE_TIME, table.column(11).type(), "Wrong TIMESTAMP_MILLIS from pyarrow");
    assertEquals(
        LocalDateTime.of(2021, 04, 23, 0, 0),
        table.dateTimeColumn(11).get(0),
        "Wrong datetime value [11,0]");
    assertEquals(
        LocalDateTime.of(2021, 04, 23, 0, 0, 1),
        table.dateTimeColumn(11).get(1),
        "Wrong datetime value [11,1]");
    assertEquals(
        ColumnType.STRING, table.column(12).type(), "Wrong string column type from pyarrow");
    assertEquals("string1", table.stringColumn(12).get(0), "Wrong string value [12,0]");
    assertEquals("string2", table.stringColumn(12).get(1), "Wrong string value [12,1]");
  }

  @Test
  void testDefaultReadFromFastparquet() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(TablesawParquetReadOptions.builder(new File(PANDAS_FASTPARQUET)).build());
    assertEquals("pandas_fastparquet.parquet", table.name(), "Wrong name from pyarrow");
    assertEquals(
        ColumnType.BOOLEAN, table.column(0).type(), "Wrong boolean column type from fastparquet");
    assertEquals(true, table.booleanColumn(0).get(0), "Wrong boolean value [0,0]");
    assertEquals(false, table.booleanColumn(0).get(1), "Wrong boolean value [0,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(1).type(), "Wrong uint8 column type from fastparquet");
    assertEquals(0, table.intColumn(1).get(0), "Wrong integer value [1,0]");
    assertEquals(127, table.intColumn(1).get(1), "Wrong integer value [1,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(2).type(), "Wrong int8 column type from fastparquet");
    assertEquals(-127, table.intColumn(2).get(0), "Wrong integer value [2,0]");
    assertEquals(1, table.intColumn(2).get(1), "Wrong integer value [2,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(3).type(), "Wrong uint16 column type from fastparquet");
    assertEquals(0, table.intColumn(3).get(0), "Wrong integer value [3,0]");
    assertEquals(32767, table.intColumn(3).get(1), "Wrong integer value [3,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(4).type(), "Wrong int16 column type from fastparquet");
    assertEquals(0, table.intColumn(4).get(0), "Wrong integer value [4,0]");
    assertEquals(-32767, table.intColumn(4).get(1), "Wrong integer value [4,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(5).type(), "Wrong uint32 column type from fastparquet");
    assertEquals(0, table.intColumn(5).get(0), "Wrong integer value [5,0]");
    assertEquals(65000, table.intColumn(5).get(1), "Wrong integer value [5,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(6).type(), "Wrong int32 column type from fastparquet");
    assertEquals(0, table.intColumn(6).get(0), "Wrong integer value [6,0]");
    assertEquals(-65000, table.intColumn(6).get(1), "Wrong integer value [6,1]");
    assertEquals(
        ColumnType.LONG, table.column(7).type(), "Wrong uint64 column type from fastparquet");
    assertEquals(0, table.longColumn(7).get(0), "Wrong long value [7,0]");
    assertEquals(1_000_000_000, table.longColumn(7).get(1), "Wrong long value [7,1]");
    assertEquals(
        ColumnType.LONG, table.column(8).type(), "Wrong int164 column type from fastparquet");
    assertEquals(0, table.longColumn(8).get(0), "Wrong long value [8,0]");
    assertEquals(-1_000_000_000, table.longColumn(8).get(1), "Wrong long value [8,1]");
    assertEquals(
        ColumnType.DOUBLE, table.column(9).type(), "Wrong float32 column type from fastparquet");
    assertEquals(null, table.doubleColumn(9).get(0), "Wrong double value [9,0]");
    assertEquals(1.0d, table.doubleColumn(9).get(1), "Wrong double value [9,1]");
    assertEquals(
        ColumnType.DOUBLE, table.column(10).type(), "Wrong float64 column type from pyarrow");
    assertEquals(0.0d, table.doubleColumn(10).get(0), "Wrong double value [10,0]");
    assertEquals(null, table.doubleColumn(10).get(1), "Wrong double value [10,1]");
    assertEquals(
        ColumnType.INSTANT,
        table.column(11).type(),
        "Wrong TIMESTAMP_MICROS column type from fastparquet");
    assertEquals(
        Instant.parse("2021-04-23T00:00:00Z"),
        table.instantColumn(11).get(0),
        "Wrong instant value [11,0]");
    assertEquals(
        Instant.parse("2021-04-23T00:00:01Z"),
        table.instantColumn(11).get(1),
        "Wrong instant value [11,1]");
    assertEquals(
        ColumnType.STRING, table.column(12).type(), "Wrong string column type from fastparquet");
    assertEquals("string1", table.stringColumn(12).get(0), "Wrong string value [12,0]");
    assertEquals("string2", table.stringColumn(12).get(1), "Wrong string value [12,1]");
  }

  @Test
  void testMinimizedReadFromFastparquet() throws IOException {
    final Table table =
        new TablesawParquetReader()
            .read(
                TablesawParquetReadOptions.builder(new File(PANDAS_FASTPARQUET))
                    .minimizeColumnSizes()
                    .build());
    assertEquals("pandas_fastparquet.parquet", table.name(), "Wrong name from pyarrow");
    assertEquals(
        ColumnType.BOOLEAN, table.column(0).type(), "Wrong boolean column type from pyarrow");
    assertEquals(true, table.booleanColumn(0).get(0), "Wrong boolean value [0,0]");
    assertEquals(false, table.booleanColumn(0).get(1), "Wrong boolean value [0,1]");
    assertEquals(ColumnType.SHORT, table.column(1).type(), "Wrong uint8 column type from pyarrow");
    assertEquals((short) 0, table.shortColumn(1).get(0), "Wrong short value [1,0]");
    assertEquals((short) 127, table.shortColumn(1).get(1), "Wrong short value [1,1]");
    assertEquals(ColumnType.SHORT, table.column(2).type(), "Wrong int8 column type from pyarrow");
    assertEquals((short) -127, table.shortColumn(2).get(0), "Wrong short value [2,0]");
    assertEquals((short) 1, table.shortColumn(2).get(1), "Wrong short value [2,1]");
    assertEquals(ColumnType.SHORT, table.column(3).type(), "Wrong uint16 column type from pyarrow");
    assertEquals((short) 0, table.shortColumn(3).get(0), "Wrong short value [3,0]");
    assertEquals((short) 32767, table.shortColumn(3).get(1), "Wrong short value [3,1]");
    assertEquals(ColumnType.SHORT, table.column(4).type(), "Wrong int16 column type from pyarrow");
    assertEquals((short) 0, table.shortColumn(4).get(0), "Wrong short value [4,0]");
    assertEquals((short) -32767, table.shortColumn(4).get(1), "Wrong short value [4,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(5).type(), "Wrong uint32 column type from pyarrow");
    assertEquals(0, table.intColumn(5).get(0), "Wrong integer value [5,0]");
    assertEquals(65000, table.intColumn(5).get(1), "Wrong integer value [5,1]");
    assertEquals(
        ColumnType.INTEGER, table.column(6).type(), "Wrong int32 column type from pyarrow");
    assertEquals(0, table.intColumn(6).get(0), "Wrong integer value [6,0]");
    assertEquals(-65000, table.intColumn(6).get(1), "Wrong integer value [6,1]");
    assertEquals(ColumnType.LONG, table.column(7).type(), "Wrong uint64 column type from pyarrow");
    assertEquals(0, table.longColumn(7).get(0), "Wrong long value [7,0]");
    assertEquals(1_000_000_000, table.longColumn(7).get(1), "Wrong long value [7,1]");
    assertEquals(ColumnType.LONG, table.column(8).type(), "Wrong int164 column type from pyarrow");
    assertEquals(0, table.longColumn(8).get(0), "Wrong long value [8,0]");
    assertEquals(-1_000_000_000, table.longColumn(8).get(1), "Wrong long value [8,1]");
    assertEquals(ColumnType.FLOAT, table.column(9).type(), "Wrong float32 from pyarrow");
    assertEquals(null, table.floatColumn(9).get(0), "Wrong float value [9,0]");
    assertEquals(1.0f, table.floatColumn(9).get(1), "Wrong float value [9,1]");
    assertEquals(ColumnType.DOUBLE, table.column(10).type(), "Wrong float64 from pyarrow");
    assertEquals(0.0d, table.doubleColumn(10).get(0), "Wrong double value [10,0]");
    assertEquals(null, table.doubleColumn(10).get(1), "Wrong double value [10,1]");
    assertEquals(
        ColumnType.INSTANT, table.column(11).type(), "Wrong TIMESTAMP_MICROS from pyarrow");
    assertEquals(
        Instant.parse("2021-04-23T00:00:00Z"),
        table.instantColumn(11).get(0),
        "Wrong instant value [11,0]");
    assertEquals(
        Instant.parse("2021-04-23T00:00:01Z"),
        table.instantColumn(11).get(1),
        "Wrong instant value [11,1]");
    assertEquals(
        ColumnType.STRING, table.column(12).type(), "Wrong string column type from fastparquet");
    assertEquals("string1", table.stringColumn(12).get(0), "Wrong string value [12,0]");
    assertEquals("string2", table.stringColumn(12).get(1), "Wrong string value [12,1]");
  }
}
