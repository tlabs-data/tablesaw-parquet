package net.tlabs.tablesaw.parquet;

public class TablesawParquet {

	private TablesawParquet() {
		super();
	}
	
	public static void register() {
		registerReader();
		registerWriter();
	}

	public static void registerReader() {
		TablesawParquetReader.register();
	}

	public static void registerWriter() {
		TablesawParquetWriter.register();
	}

}
