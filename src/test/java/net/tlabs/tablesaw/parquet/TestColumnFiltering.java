package net.tlabs.tablesaw.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.Table;

class TestColumnFiltering {

    private static final String APACHE_ALL_TYPES_SNAPPY =
        "target/test/data/parquet-testing-master/data/alltypes_plain.snappy.parquet";
    
    @Test
    void testKeepFirstSix() throws IOException {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_SNAPPY)
                .withOnlyTheseColumns("id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col")
                .build());
        assertEquals(6, table.columnCount(), "Wrong number of column");
    }

    @Test
    void testKeepLastFive() throws IOException {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_SNAPPY)
                .withOnlyTheseColumns("float_col", "double_col", "date_string_col", "string_col", "timestamp_col")
                .build());
        assertEquals(5, table.columnCount(), "Wrong number of column");
    }
    
    @Test
    void testColumnOrdering() throws IOException {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(APACHE_ALL_TYPES_SNAPPY)
                .withOnlyTheseColumns("float_col", "id")
                .build());
        assertEquals("float_col", table.column(0).name());
        assertEquals("id", table.column(1).name());
    }

}
