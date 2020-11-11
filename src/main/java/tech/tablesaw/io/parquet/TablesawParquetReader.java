package tech.tablesaw.io.parquet;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.DataReader;
import tech.tablesaw.io.ReaderRegistry;
import tech.tablesaw.io.Source;

public class TablesawParquetReader implements DataReader<TablesawParquetReadOptions> {
	
	private static final Logger LOG = LoggerFactory.getLogger(TablesawParquetReader.class);

	private static final TablesawParquetReader INSTANCE = new TablesawParquetReader();
	
	static {
		register(Table.defaultReaderRegistry);
	}

	public static void register(ReaderRegistry registry) {
		registry.registerExtension("parquet", INSTANCE);
		registry.registerMimeType("parquet", INSTANCE); // TODO: find parquet mime type
		registry.registerOptions(TablesawParquetReadOptions.class, INSTANCE);
	}

	@Override
	public Table read(final Source source) throws IOException {
		return read(TablesawParquetReadOptions.builder(source).build());
	}

	@Override
	public Table read(final TablesawParquetReadOptions options) throws IOException {
		final long start = System.currentTimeMillis();
		final File file = options.source().file();
		final Path path = new Path(file.getAbsolutePath());
		final TablesawReadSupport readSupport = new TablesawReadSupport(options);
		try (final ParquetReader<Row> reader = ParquetReader.<Row>builder(readSupport, path).build()) {
			int i = 0;
			while(reader.read() != null) {
				i++;
			}
			final long end = System.currentTimeMillis();
			LOG.debug("Finished reading {} rows from {} in {} ms", i, file.getName(), (end - start));
		}
		return readSupport.getTable();
	}
}
