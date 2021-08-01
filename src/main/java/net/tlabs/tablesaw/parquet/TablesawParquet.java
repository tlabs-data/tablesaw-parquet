package net.tlabs.tablesaw.parquet;

public class TablesawParquet {

	private TablesawParquet() {
		super();
	}

	/**
	 * Register the TablesawParquetReader and TablesawParquetWriter
	 * in the Tablesaw registries.
	 */
	public static void register() {
		registerReader();
		registerWriter();
	}

	/**
	 * Register the TablesawParquetReader
	 * in the Tablesaw registries.
	 */
	public static void registerReader() {
		TablesawParquetReader.register();
	}

	/**
	 * Register the TablesawParquetWriter
	 * in the Tablesaw registries.
	 */
	public static void registerWriter() {
		TablesawParquetWriter.register();
	}

}
