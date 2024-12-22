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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import net.tlabs.tablesaw.parquet.TablesawParquetWriteOptions.CompressionCodec;
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
import tech.tablesaw.io.RuntimeIOException;

class TestParquetWriter {

    private static final String PARQUET_TESTING_FOLDER = "target/test/data/parquet-testing-master/data/";

    private static final String APACHE_ALL_TYPES_DICT = "alltypes_dictionary.parquet";
    private static final String APACHE_ALL_TYPES_PLAIN = "alltypes_plain.parquet";
    private static final String APACHE_ALL_TYPES_SNAPPY = "alltypes_plain.snappy.parquet";
    private static final String APACHE_FIXED_LENGTH_DECIMAL = "fixed_length_decimal.parquet";
    private static final String APACHE_BYTE_ARRAY_DECIMAL = "byte_array_decimal.parquet";
    private static final String APACHE_DATAPAGEV2 = "datapage_v2.snappy.parquet";
    private static final String OUTPUT_FILE = "target/test/results/out.parquet";
    private static final String OUTPUT_CRC_FILE = "target/test/results/.out.parquet.crc";

    private static final TablesawParquetWriter PARQUET_WRITER = new TablesawParquetWriter();
    private static final TablesawParquetReader PARQUET_READER = new TablesawParquetReader();

    public static void assertTableEquals(final Table expected, final Table actual, final String header) {
        final int numberOfColumns = actual.columnCount();
        assertEquals(expected.columnCount(), numberOfColumns, header + " tables should have same number of columns");
        for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
            final Column<?> actualColumn = actual.column(columnIndex);
            final Column<?> expectedColumn = expected.column(columnIndex);
            assertEquals(expectedColumn.name(), actualColumn.name(), "Wrong column name");
            // No good way to distinguish between string and text after writing
            if (actualColumn instanceof StringColumn) {
                assertTrue(expectedColumn instanceof StringColumn || expectedColumn instanceof TextColumn,
                    "Column transformed to StringColumns");
            } else {
                assertEquals(expectedColumn.type(), actualColumn.type(), "Column type different");
            }
        }
        final int numberOfRows = actual.rowCount();
        assertEquals(expected.rowCount(), numberOfRows, header + " tables should have same number of rows");
        for (int rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
            for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
                assertEquals(expected.get(rowIndex, columnIndex), actual.get(rowIndex, columnIndex),
                    header + " cells[" + rowIndex + ", " + columnIndex + "] do not match");
            }
        }
    }

    @Test
    void testReadWriteAllTypeDict() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_DICT).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_DICT + " reloaded");
    }

    @Test
    void testReadWriteAllTypeDictMinimized() {
        final Table orig = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_DICT).minimizeColumnSizes().build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(OUTPUT_FILE).minimizeColumnSizes().build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_DICT + " reloaded");
    }

    @Test
    void testReadWriteAllTypeDictToFile() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_DICT).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(new File(OUTPUT_FILE)).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_DICT + " reloaded");
    }

    @Test
    void testReadWriteAllTypePlain() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " reloaded");
    }

    @Test
    void testReadWriteAllTypeSnappy() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_SNAPPY).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_SNAPPY + " reloaded");
    }

    @Test
    void testInt96AsTimestamp() {
        final Table orig = PARQUET_READER.read(TablesawParquetReadOptions
            .builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).withConvertInt96ToTimestamp(true).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " with Int96 as Timestamp reloaded");
    }

    @Test
    void testFixedLengthDecimal() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_FIXED_LENGTH_DECIMAL).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_FIXED_LENGTH_DECIMAL + " reloaded");
    }

    @Test
    void testBinaryDecimal() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_BYTE_ARRAY_DECIMAL).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_BYTE_ARRAY_DECIMAL + " reloaded");
    }

    @Test
    void testDatapageV2() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_DATAPAGEV2).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_DATAPAGEV2 + " reloaded");
    }

    @Test
    void testOverwriteOption() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final TablesawParquetWriteOptions options = TablesawParquetWriteOptions
            .builder(OUTPUT_FILE)
            .withOverwrite(false).build();
        assertThrows(RuntimeIOException.class, () -> PARQUET_WRITER.write(orig, options));
    }

    @Test
    void testGZIPCompressor() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).build());
        PARQUET_WRITER.write(orig,
            TablesawParquetWriteOptions.builder(OUTPUT_FILE).withCompressionCode(CompressionCodec.GZIP).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " gzip reloaded");
    }

    @Test
    void testPLAINCompressor() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).build());
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE)
            .withCompressionCode(CompressionCodec.UNCOMPRESSED).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " plain reloaded");
    }

    @Test
    void testSNAPPYCompressor() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).build());
        PARQUET_WRITER.write(orig,
            TablesawParquetWriteOptions.builder(OUTPUT_FILE).withCompressionCode(CompressionCodec.SNAPPY).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " snappy reloaded");
    }

    @Test
    void testZSTDCompressor() {
        final Table orig = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN).build());
        PARQUET_WRITER.write(orig,
            TablesawParquetWriteOptions.builder(OUTPUT_FILE).withCompressionCode(CompressionCodec.ZSTD).build());
        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, APACHE_ALL_TYPES_PLAIN + " zstd reloaded");
    }

    @Test
    void testDestinationWriteException() {
        assertThrows(UnsupportedOperationException.class, () -> PARQUET_WRITER.write(null, (Destination) null),
            "Wrong exception on writing to destination");
    }

    @Test
    void testWriteReadAllColumnTypes() {
        final Table orig = Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2), 
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0L, 500_000_000_000L),
            FloatColumn.create("float", Float.NaN, 2.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d), 
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table dest = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(OUTPUT_FILE).minimizeColumnSizes().build());
        assertTableEquals(orig, dest, "All ColumnTypes reloaded");
    }

    @Test
    void testWriteReadDefaultTypes() {
        final Table orig = Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0L, 500_000_000_000L),
            FloatColumn.create("float", Float.NaN, 2.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d),
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());

        orig.replaceColumn("short", orig.shortColumn("short").asIntColumn().setName("short"));
        orig.replaceColumn("float", orig.floatColumn("float").asDoubleColumn().setName("float"));

        final Table dest = PARQUET_READER.read(TablesawParquetReadOptions.builder(OUTPUT_FILE).build());
        assertTableEquals(orig, dest, "All ColumnTypes reloaded");
    }

    @Test
    void testWriteReadEmptyColumnTypeList() {
        final Table orig = Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0L, 500_000_000_000L),
            FloatColumn.create("float", Float.NaN, 2.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d),
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());

        orig.replaceColumn("short", orig.shortColumn("short").asIntColumn().setName("short"));
        orig.replaceColumn("float", orig.floatColumn("float").asDoubleColumn().setName("float"));

        final Table dest = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(OUTPUT_FILE).columnTypesToDetect(new ArrayList<>()).build());
        assertTableEquals(orig, dest, "All ColumnTypes reloaded");
    }

    @Test
    void testWriteReadNoShorts() {
        final Table orig = Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0L, 500_000_000_000L),
            FloatColumn.create("float", Float.NaN, 2.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d),
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());

        orig.replaceColumn("short", orig.shortColumn("short").asIntColumn().setName("short"));

        final List<ColumnType> types = new ArrayList<>();
        types.add(ColumnType.FLOAT);
        final Table dest = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(OUTPUT_FILE).columnTypesToDetect(types).build());
        assertTableEquals(orig, dest, "All ColumnTypes reloaded");
    }

    @Test
    void testWriteReadNoFloat() {
        final Table orig = Table.create(
            BooleanColumn.create("boolean", true, false),
            DateColumn.create("date", LocalDate.now(), LocalDate.now()),
            DateTimeColumn.create("datetime", LocalDateTime.now(), LocalDateTime.now()),
            InstantColumn.create("instant", Instant.now(), Instant.now()),
            TimeColumn.create("time", LocalTime.now(), LocalTime.NOON),
            ShortColumn.create("short", (short) 0, (short) 2),
            IntColumn.create("integer", 1, 255),
            LongColumn.create("long", 0L, 500_000_000_000L),
            FloatColumn.create("float", Float.NaN, 2.14159f),
            DoubleColumn.create("double", Double.MAX_VALUE, 0.0d), 
            StringColumn.create("string", "", "abdce"),
            TextColumn.create("text", "abdceabdceabdceabdceabdceabdceabdceabdceabdce", ""));

        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());

        orig.replaceColumn("float", orig.floatColumn("float").asDoubleColumn().setName("float"));

        final List<ColumnType> types = new ArrayList<>();
        types.add(ColumnType.SHORT);
        final Table dest = PARQUET_READER
            .read(TablesawParquetReadOptions.builder(OUTPUT_FILE).columnTypesToDetect(types).build());
        assertTableEquals(orig, dest, "All ColumnTypes reloaded");
    }
    
    /**
     * checksum file is removed when option is false
     * checksum tests can be run in any order
     */
    @Test
    void testWriteDefaultNoChecksum() {
        final Table orig = Table.create(BooleanColumn.create("boolean", true, false));
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final File checksumFile = new File(OUTPUT_CRC_FILE);
        assertFalse(checksumFile.exists(), "Default option for checksum is not false");
    }

    @Test
    void testWriteOptionWithChecksum() {
        final Table orig = Table.create(BooleanColumn.create("boolean", true, false));
        PARQUET_WRITER.write(orig, TablesawParquetWriteOptions.builder(OUTPUT_FILE).withWriteChecksum(true).build());
        final File checksumFile = new File(OUTPUT_CRC_FILE);
        assertTrue(checksumFile.exists(), "Option with checksum does not generate checksum");
    }
}
