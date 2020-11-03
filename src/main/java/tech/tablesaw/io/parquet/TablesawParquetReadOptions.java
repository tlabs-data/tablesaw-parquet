package tech.tablesaw.io.parquet;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.io.Source;

public class TablesawParquetReadOptions extends ReadOptions {

	public enum ManageGroupsAs {
		TEXT,
		SKIP,
		ERROR
	}
	
	protected boolean convertInt96ToTimestamp;
	protected boolean unnanotatedBinaryAsString;
	protected ManageGroupsAs manageGroupsAs;
	
	protected TablesawParquetReadOptions(final Builder builder) {
		super(builder);
		convertInt96ToTimestamp = builder.convertInt96ToTimestamp;
		unnanotatedBinaryAsString = builder.unnanotatedBinaryAsString;
		manageGroupsAs = builder.manageGroupsAs;
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

	public static Builder builder(final Source source) {
		return new Builder(source);
	}

	public static Builder builder(final File file) {
		return new Builder(file).tableName(file.getName());
	}

	public static Builder builder(final String fileName) {
		return new Builder(new File(fileName));
	}

	public static Builder builder(final URL url) throws IOException {
		return new Builder(url);
	}

	public static Builder builderFromFile(final String fileName) {
		return new Builder(new File(fileName));
	}

	public static Builder builderFromString(final String contents) {
		return new Builder(new StringReader(contents));
	}

	public static Builder builderFromUrl(final String url) throws IOException {
		return new Builder(new URL(url));
	}

	public static class Builder extends ReadOptions.Builder {
		protected boolean convertInt96ToTimestamp = false;
		protected boolean unnanotatedBinaryAsString = true;
		protected ManageGroupsAs manageGroupsAs = ManageGroupsAs.TEXT;
		protected Builder(final Source source) {
			super(source);
		}

		protected Builder(final URL url) throws IOException {
			super(url);
		}

		protected Builder(final File file) {
			super(file);
		}

		protected Builder(final Reader reader) {
			super(reader);
		}

		protected Builder(final InputStream stream) {
			super(stream);
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
