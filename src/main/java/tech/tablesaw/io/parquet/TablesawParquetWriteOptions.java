package tech.tablesaw.io.parquet;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import tech.tablesaw.io.WriteOptions;

public class TablesawParquetWriteOptions extends WriteOptions {

  protected final String outputFile;

  public static Builder builder(final File file) throws IOException {
    return new Builder(file.getAbsolutePath());
  }

  public static Builder builder(final String outputFile) throws IOException {
    return new Builder(outputFile);
  }

  protected TablesawParquetWriteOptions(final Builder builder) {
    super(builder);
    this.outputFile = builder.outputFile;
  }

  public static class Builder extends WriteOptions.Builder {

    protected final String outputFile;

    public Builder(final String outputFile) throws IOException {
      super((Writer) null);
      this.outputFile = outputFile;
    }

    public TablesawParquetWriteOptions build() {
      return new TablesawParquetWriteOptions(this);
    }
  }
}
