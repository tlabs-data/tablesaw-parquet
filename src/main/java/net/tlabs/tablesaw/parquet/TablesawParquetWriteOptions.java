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

import java.io.File;
import java.io.Writer;
import tech.tablesaw.io.WriteOptions;

public class TablesawParquetWriteOptions extends WriteOptions {

	public enum CompressionCodec {
		UNCOMPRESSED, SNAPPY, GZIP, ZSTD
	}

	protected final String outputFile;
	protected final CompressionCodec compressionCodec;
	protected final boolean overwrite;

	public static Builder builder(final File file) {
		return new Builder(file.getAbsolutePath());
	}

	public static Builder builder(final String outputFile) {
		return new Builder(outputFile);
	}

	protected TablesawParquetWriteOptions(final Builder builder) {
		super(builder);
		this.outputFile = builder.outputFile;
		this.compressionCodec = builder.compressionCodec;
		this.overwrite = builder.overwrite;
	}

	public static class Builder extends WriteOptions.Builder {

		protected final String outputFile;
		protected CompressionCodec compressionCodec = CompressionCodec.SNAPPY;
		protected boolean overwrite = true;

		public Builder(final String outputFile) {
			super((Writer) null);
			this.outputFile = outputFile;
		}

		public Builder withCompressionCode(final CompressionCodec compressionCodec) {
			this.compressionCodec = compressionCodec;
			return this;
		}

		public Builder withOverwrite(final boolean overwrite) {
			this.overwrite = overwrite;
			return this;
		}

		public TablesawParquetWriteOptions build() {
			return new TablesawParquetWriteOptions(this);
		}
	}
}
