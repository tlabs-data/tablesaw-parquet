package tech.tablesaw.io.parquet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import tech.tablesaw.io.WriteOptions;

public class TablesawParquetWriteOptions extends WriteOptions {

  public enum CompressionCodec {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    ZSTD
  }

  protected final String outputFile;
  protected final CompressionCodec compressionCodec;
  protected final boolean overwrite;

  public static Builder builder(final File file) throws IOException {
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
