package net.tlabs.tablesaw.parquet;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.Table;

class TestRowFiltering {

    private static final String PARQUET_TESTING_FOLDER = "target/test/data/parquet-testing-master/data/";
    private static final String APACHE_ALL_TYPES_PLAIN = "alltypes_plain.parquet";
    private static final TablesawParquetReader PARQUET_READER = new TablesawParquetReader();

    @Test
    void testBasicFilter() {
        final TablesawParquetReadOptions options =
            TablesawParquetReadOptions.builder(PARQUET_TESTING_FOLDER + APACHE_ALL_TYPES_PLAIN)
            .withRecordFilter(eq(booleanColumn("bool_col"), Boolean.TRUE))
            .build();
        final Table table = PARQUET_READER.read(options);
        assertEquals(4, table.rowCount());
    }

    //TODO: add more testing
}
