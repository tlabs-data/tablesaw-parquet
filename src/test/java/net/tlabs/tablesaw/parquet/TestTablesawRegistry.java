package net.tlabs.tablesaw.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tech.tablesaw.api.Table;

class TestTablesawRegistry {

    private static final String PANDAS_PYARROW = "target/test-classes/pandas_pyarrow.parquet";
    private static final String OUTPUT_FILE = "target/test/results/out.parquet";

    @BeforeAll
    static void init() {
        TablesawParquet.register();
    }

    @Test
    void testRegistryReadFilename() throws IOException {
        final Table table = Table.read().file(PANDAS_PYARROW);
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadFile() throws IOException {
        final Table table = Table.read().file(new File(PANDAS_PYARROW));
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadUsingOptionsBuilder() throws IOException {
        final Table table = Table.read().usingOptions(TablesawParquetReadOptions.builder(PANDAS_PYARROW));
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadUsingOptions() throws IOException {
        final Table table = Table.read().usingOptions(TablesawParquetReadOptions.builder(PANDAS_PYARROW).build());
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryReadString() {
        assertThrows(UnsupportedOperationException.class, () -> Table.read().string("STRING", "parquet"));
    }

    @Test
    void testRegistryReadURL() {
        assertThrows(UnsupportedOperationException.class,
            () -> Table.read().url(new File(PANDAS_PYARROW).toURI().toURL()));
    }

    @Test
    void testRegistryReadURLUsingOptions() throws IOException {
        final Table table = Table.read()
            .usingOptions(TablesawParquetReadOptions.builder(new File(PANDAS_PYARROW).toURI().toURL()));
        assertNotNull(table, "Read table is null");
    }

    @Test
    void testRegistryWriteUsingOptions() throws IOException {
        final Table table = Table.read().file(PANDAS_PYARROW);
        table.write().usingOptions(TablesawParquetWriteOptions.builder(OUTPUT_FILE).build());
        final Table readTable = Table.read().file(OUTPUT_FILE);
        assertNotNull(readTable, "Read table is null");
    }

    @Test
    void testRegistryWriteFilename() throws IOException {
        final Table table = Table.read().file(PANDAS_PYARROW);
        assertThrows(NullPointerException.class, () -> table.write().toFile(OUTPUT_FILE));
    }

    @Test
    void testRegistryWriteFile() throws IOException {
        final Table table = Table.read().file(PANDAS_PYARROW);
        assertThrows(NullPointerException.class, () -> table.write().toFile(new File(OUTPUT_FILE)));
    }
}
