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
import java.net.URL;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.io.ReadOptions;

public class TablesawParquetReadOptions extends ReadOptions {

    public enum ManageGroupsAs {
        TEXT, SKIP, ERROR
    }

    public enum UnnanotatedBinaryAs {
        STRING, HEXSTRING, SKIP
    }

    private static final Logger LOG = LoggerFactory.getLogger(TablesawParquetReadOptions.class);

    private final boolean convertInt96ToTimestamp;
    private final UnnanotatedBinaryAs unnanotatedBinaryAs;
    private final boolean shortColumnTypeUsed;
    private final boolean floatColumnTypeUsed;
    private final ManageGroupsAs manageGroupsAs;
    private final String inputPath;

    protected TablesawParquetReadOptions(final Builder builder) {
        super(builder);
        convertInt96ToTimestamp = builder.convertInt96ToTimestamp;
        unnanotatedBinaryAs = builder.unnanotatedBinaryAs;
        manageGroupsAs = builder.manageGroupsAs;
        inputPath = builder.inputPath;
        shortColumnTypeUsed = this.columnTypesToDetect.contains(ColumnType.SHORT);
        floatColumnTypeUsed = this.columnTypesToDetect.contains(ColumnType.FLOAT);
    }

    public boolean isShortColumnTypeUsed() {
        return shortColumnTypeUsed;
    }

    public boolean isFloatColumnTypeUsed() {
        return floatColumnTypeUsed;
    }

    public boolean isConvertInt96ToTimestamp() {
        return convertInt96ToTimestamp;
    }

    public UnnanotatedBinaryAs getUnnanotatedBinaryAs() {
        return unnanotatedBinaryAs;
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

    public static Builder builder(final URL url) {
        final String urlString = url.toString();
        return new Builder(urlString).tableName(urlString);
    }

    public static class Builder extends ReadOptions.Builder {
        private boolean convertInt96ToTimestamp = false;
        private UnnanotatedBinaryAs unnanotatedBinaryAs = UnnanotatedBinaryAs.STRING;
        private ManageGroupsAs manageGroupsAs = ManageGroupsAs.TEXT;
        private final String inputPath;

        protected Builder(final String inputPath) {
            super();
            this.inputPath = inputPath;
        }

        @Override
        public TablesawParquetReadOptions build() {
            return new TablesawParquetReadOptions(this);
        }

        // Override super-class setters to return an instance of this class

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
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

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder sample(final boolean sample) {
            LOG.warn("Sampling is not used in TablesawParquetReadOptions");
            super.sample(sample);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        @Deprecated
        public Builder dateFormat(final String dateFormat) {
            super.dateFormat(dateFormat);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        @Deprecated
        public Builder timeFormat(final String timeFormat) {
            super.timeFormat(timeFormat);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        @Deprecated
        public Builder dateTimeFormat(final String dateTimeFormat) {
            super.dateTimeFormat(dateTimeFormat);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder dateFormat(final DateTimeFormatter dateFormat) {
            super.dateFormat(dateFormat);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder timeFormat(final DateTimeFormatter timeFormat) {
            super.timeFormat(timeFormat);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder dateTimeFormat(final DateTimeFormatter dateTimeFormat) {
            super.dateTimeFormat(dateTimeFormat);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder maxCharsPerColumn(final int maxCharsPerColumn) {
            super.maxCharsPerColumn(maxCharsPerColumn);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder locale(final Locale locale) {
            super.locale(locale);
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder missingValueIndicator(final String... missingValueIndicator) {
            LOG.warn("Missing value indicator is not used in TablesawParquetReadOptions");
            super.missingValueIndicator(missingValueIndicator);
            return this;
        }

        /**
         * This option can be used to select whether to use:
         * ShortColumn or IntColumn for parquet short and byte columns.
         * FloatColumn or DoubleColumn for parquet float columns.
         * If the list does not contain ColumnType.SHORT, an IntColumn will be used for parquet short and byte columns.
         * If the list does not contain ColumnType.FLOAT, a DoubleColumn will be used for parquet float columns.
         *
         * @param columnTypesToDetect only checked for presence of ColumnType.SHORT and ColumnType.FLOAT
         * @return this builder
         */
        @Override
        public Builder columnTypesToDetect(final List<ColumnType> columnTypesToDetect) {
            super.columnTypesToDetect(columnTypesToDetect);
            return this;
        }

        @Override
        public Builder minimizeColumnSizes() {
            super.minimizeColumnSizes();
            return this;
        }

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder ignoreZeroDecimal(final boolean ignoreZeroDecimal) {
            LOG.warn("ignoreZeroDecimal has no effect in TablesawParquetReadOptions");
            super.ignoreZeroDecimal(ignoreZeroDecimal);
            return this;
        }

        @Override
        public Builder allowDuplicateColumnNames(Boolean allow) {
            // TODO Auto-generated method stub
            super.allowDuplicateColumnNames(allow);
            return this;
        }

        @Override
        public Builder skipRowsWithInvalidColumnCount(boolean skipRowsWithInvalidColumnCount) {
            // TODO Auto-generated method stub
            super.skipRowsWithInvalidColumnCount(skipRowsWithInvalidColumnCount);
            return this;
        }

        @Override
        public Builder columnTypes(ColumnType[] columnTypes) {
            // TODO Auto-generated method stub
            super.columnTypes(columnTypes);
            return this;
        }

        @Override
        public Builder columnTypes(Function<String, ColumnType> columnTypeFunction) {
            // TODO Auto-generated method stub
            super.columnTypes(columnTypeFunction);
            return this;
        }

        @Override
        public Builder columnTypesPartial(Function<String, Optional<ColumnType>> columnTypeFunction) {
            // TODO Auto-generated method stub
            super.columnTypesPartial(columnTypeFunction);
            return this;
        }

        @Override
        public Builder columnTypesPartial(Map<String, ColumnType> columnTypeByName) {
            // TODO Auto-generated method stub
            super.columnTypesPartial(columnTypeByName);
            return this;
        }

        /**
         * Option to read parquet INT96 values as TimeStamp. False by default.
         *
         * @param convertInt96ToTimestamp set to true to read parquet INT96 values as TimeStamp, false to read as String.
         * @return this builder
         */
        public Builder withConvertInt96ToTimestamp(final boolean convertInt96ToTimestamp) {
            this.convertInt96ToTimestamp = convertInt96ToTimestamp;
            return this;
        }

        /**
         * Option for managing unnanotated parquet Binary.
         * With UnnanotatedBinaryAs.STRING, these binaries are converted to UTF-8 Strings.
         * With UnnanotatedBinaryAs.HEXSTRING, these binaries are converted to hexadecimal Strings.
         * With UnnanotatedBinaryAs.SKIP, these fields are skipped.
         *
         * @param unnanotatedBinaryAs the UnnanotatedBinaryAs option
         * @return this builder
         */
        public Builder withUnnanotatedBinaryAs(final UnnanotatedBinaryAs unnanotatedBinaryAs) {
            this.unnanotatedBinaryAs = unnanotatedBinaryAs;
            return this;
        }

        /**
         * Option for managing parquet groups (incl. repeats).
         * With ManageGroupsAs.TEXT, groups are converted to String columns (default behavior).
         * With ManageGroupsAs.SKIP, groups are ignored.
         * With ManageGroupsAs.ERROR, reading a parquet file containing groups will throw an exception.
         *
         * @param manageGroupsAs the ManageGroupsAs option
         * @return this builder
         */
        public Builder withManageGroupAs(final ManageGroupsAs manageGroupsAs) {
            this.manageGroupsAs = manageGroupsAs;
            return this;
        }
    }
}
