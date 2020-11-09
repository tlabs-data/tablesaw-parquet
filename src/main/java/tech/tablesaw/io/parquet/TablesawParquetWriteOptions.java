package tech.tablesaw.io.parquet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import tech.tablesaw.io.WriteOptions;

public class TablesawParquetWriteOptions extends WriteOptions {

	protected final File destFile; 

	public static Builder builder(final File file) throws IOException {
		return new Builder(file);
	}
	
	public static Builder builder(final String fileName) throws IOException {
		return new Builder(new File(fileName));
	}

	protected TablesawParquetWriteOptions(final Builder builder) {
		super(builder);
		this.destFile = builder.destFile;
	}

	public static class Builder extends WriteOptions.Builder {

		protected final File destFile; 
		
		public Builder(final File destination) throws IOException {
			super((Writer)null);
			this.destFile = destination;
		}

		public TablesawParquetWriteOptions build() {
			return new TablesawParquetWriteOptions(this);
		}
		
	}
	
}
