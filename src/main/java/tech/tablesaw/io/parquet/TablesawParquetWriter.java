package tech.tablesaw.io.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.DataWriter;
import tech.tablesaw.io.Destination;

public class TablesawParquetWriter implements DataWriter<TablesawParquetWriteOptions> {

	@Override
	public void write(final Table table, final Destination dest) throws IOException {
		throw new UnsupportedOperationException(
				"The use of Destination is not yet supported, please use the write(Table, TablesawParquetWriteOptions) method");
	}

	@Override
	public void write(final Table table, final TablesawParquetWriteOptions options) throws IOException {
		try(final ParquetWriter<Row> writer = new Builder(new Path(options.destFile.getAbsolutePath()), table)
				.withWriteMode(Mode.OVERWRITE).build()) {
			Row row = new Row(table);
			while(row.hasNext()) {
				row = row.next();
				writer.write(row);
			}
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
