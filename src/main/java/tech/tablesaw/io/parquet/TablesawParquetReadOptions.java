package tech.tablesaw.io.parquet;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import tech.tablesaw.io.ReadOptions;

public class TablesawParquetReadOptions extends ReadOptions {

  public enum ManageGroupsAs {
    TEXT,
    SKIP,
    ERROR
  }

  protected boolean convertInt96ToTimestamp;
  protected boolean unnanotatedBinaryAsString;
  protected ManageGroupsAs manageGroupsAs;
  protected String inputPath;

  protected TablesawParquetReadOptions(final Builder builder) {
    super(builder);
    convertInt96ToTimestamp = builder.convertInt96ToTimestamp;
    unnanotatedBinaryAsString = builder.unnanotatedBinaryAsString;
    manageGroupsAs = builder.manageGroupsAs;
    inputPath = builder.inputPath;
  }

  public boolean isConvertInt96ToTimestamp() {
    return convertInt96ToTimestamp;
  }

  public boolean unnanotatedBinaryAsString() {
    return unnanotatedBinaryAsString;
  }

  public ManageGroupsAs getManageGroupsAs() {
    return manageGroupsAs;
  }

  public String getInputPath() {
    return inputPath;
  }

  public static Builder builder(final File file) {
    return new Builder(file.getAbsolutePath()).tableName(file.getName());
  }

  public static Builder builder(final String inputPath) {
    return new Builder(inputPath).tableName(inputPath);
  }

  public static Builder builder(final URL url) throws IOException {
    final String urlString = url.toString();
    return new Builder(urlString).tableName(urlString);
  }

  public static class Builder extends ReadOptions.Builder {
    protected boolean convertInt96ToTimestamp = false;
    protected boolean unnanotatedBinaryAsString = true;
    protected ManageGroupsAs manageGroupsAs = ManageGroupsAs.TEXT;
    protected String inputPath;

    protected Builder(final String inputPath) {
      super();
      this.inputPath = inputPath;
    }

    @Override
    public TablesawParquetReadOptions build() {
      return new TablesawParquetReadOptions(this);
    }

    // Override super-class setters to return an instance of this class

    @Override
    public Builder header(final boolean header) {
      super.header(header);
      return this;
    }

    @Override
    public Builder tableName(final String tableName) {
      super.tableName(tableName);
      return this;
    }

    @Override
    public Builder sample(final boolean sample) {
      super.sample(sample);
      return this;
    }

    @Override
    @Deprecated
    public Builder dateFormat(final String dateFormat) {
      super.dateFormat(dateFormat);
      return this;
    }

    @Override
    @Deprecated
    public Builder timeFormat(final String timeFormat) {
      super.timeFormat(timeFormat);
      return this;
    }

    @Override
    @Deprecated
    public Builder dateTimeFormat(final String dateTimeFormat) {
      super.dateTimeFormat(dateTimeFormat);
      return this;
    }

    @Override
    public Builder dateFormat(final DateTimeFormatter dateFormat) {
      super.dateFormat(dateFormat);
      return this;
    }

    @Override
    public Builder timeFormat(final DateTimeFormatter timeFormat) {
      super.timeFormat(timeFormat);
      return this;
    }

    @Override
    public Builder dateTimeFormat(final DateTimeFormatter dateTimeFormat) {
      super.dateTimeFormat(dateTimeFormat);
      return this;
    }

    @Override
    public Builder maxCharsPerColumn(final int maxCharsPerColumn) {
      super.maxCharsPerColumn(maxCharsPerColumn);
      return this;
    }

    @Override
    public Builder locale(final Locale locale) {
      super.locale(locale);
      return this;
    }

    @Override
    public Builder missingValueIndicator(final String missingValueIndicator) {
      super.missingValueIndicator(missingValueIndicator);
      return this;
    }

    @Override
    public Builder minimizeColumnSizes() {
      super.minimizeColumnSizes();
      return this;
    }

    @Override
    public Builder ignoreZeroDecimal(final boolean ignoreZeroDecimal) {
      super.ignoreZeroDecimal(ignoreZeroDecimal);
      return this;
    }

    public Builder withConvertInt96ToTimestamp(final boolean convertInt96ToTimestamp) {
      this.convertInt96ToTimestamp = convertInt96ToTimestamp;
      return this;
    }

    public Builder withUnnanotatedBinaryAsString(final boolean unnanotatedBinaryAsString) {
      this.unnanotatedBinaryAsString = unnanotatedBinaryAsString;
      return this;
    }

    public Builder withManageGroupAs(final ManageGroupsAs manageGroupsAs) {
      this.manageGroupsAs = manageGroupsAs;
      return this;
    }
  }
}
