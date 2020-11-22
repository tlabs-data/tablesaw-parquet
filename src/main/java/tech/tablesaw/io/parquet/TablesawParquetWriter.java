package tech.tablesaw.io.parquet;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.DataWriter;
import tech.tablesaw.io.Destination;
import tech.tablesaw.io.WriterRegistry;

public class TablesawParquetWriter implements DataWriter<TablesawParquetWriteOptions> {

  private static final Logger LOG = LoggerFactory.getLogger(TablesawParquetWriter.class);

  private static final TablesawParquetWriter INSTANCE = new TablesawParquetWriter();

  static {
    register(Table.defaultWriterRegistry);
  }

  public static void register(final WriterRegistry registry) {
    registry.registerOptions(TablesawParquetWriteOptions.class, INSTANCE);
  }

  @Override
  public void write(final Table table, final Destination dest) throws IOException {
    throw new UnsupportedOperationException(
        "The use of Destination is not yet supported, please use the write(Table, TablesawParquetWriteOptions) method");
  }

  @Override
  public void write(final Table table, final TablesawParquetWriteOptions options)
      throws IOException {
    try (final ParquetWriter<Row> writer =
        new Builder(new Path(options.outputFile), table).withWriteMode(Mode.OVERWRITE).build()) {
      final long start = System.currentTimeMillis();
      Row row = new Row(table);
      while (row.hasNext()) {
        row = row.next();
        writer.write(row);
      }
      final long end = System.currentTimeMillis();
      LOG.debug(
          "Finished writing {} rows to {} in {} ms",
          row.getRowNumber() + 1,
          options.outputFile,
          (end - start));
    }
  }

  private class Builder extends ParquetWriter.Builder<Row, Builder> {

    private final Table table;

    protected Builder(final Path path, final Table table) {
      super(path);
      this.table = table;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected WriteSupport<Row> getWriteSupport(final Configuration conf) {
      return new TablesawWriteSupport(this.table);
    }
  }
}
