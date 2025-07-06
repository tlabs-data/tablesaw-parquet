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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;

import org.apache.parquet.crypto.AADPrefixVerifier;
import org.apache.parquet.crypto.ColumnDecryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.io.ReadOptions;

/**
 * Options for reading parquet file in tablesaw.
 * Use the static {@code builder} methods
 */
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
    private final List<String> columns;
    private final URI inputURI;
    private final FileDecryptionProperties fileDecryptionProperties;
    private final Filter recordFilter;

    protected TablesawParquetReadOptions(final Builder builder) {
        super(builder);
        convertInt96ToTimestamp = builder.convertInt96ToTimestamp;
        unnanotatedBinaryAs = builder.unnanotatedBinaryAs;
        manageGroupsAs = builder.manageGroupsAs;
        columns = Collections.unmodifiableList(Arrays.asList(builder.columns));
        inputURI = builder.inputURI;
        shortColumnTypeUsed = this.columnTypesToDetect.contains(ColumnType.SHORT);
        floatColumnTypeUsed = this.columnTypesToDetect.contains(ColumnType.FLOAT);
        fileDecryptionProperties = builder.getFileDecryptionProperties();
        recordFilter = builder.recordFilter;
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

    /**
     * Returns the list of column names to read.
     * An empty list means to read all columns. 
     * @return Immutable list of column names to read.
     */
    public List<String> getColumns() {
        return columns;
    }
    
    /**
     * Returns whether the given column name must be read.
     * @param columnName the column name
     * @return true if the column must be read, false otherwise.
     */
    public boolean hasColumn(final String columnName) {
        if(columns.isEmpty()) return true;
        return columns.contains(columnName);
    }

    public URI getInputURI() {
        return inputURI;
    }

    public String getSanitizedinputPath() {
        return sanitize(inputURI);
    }

    private static String sanitize(final URI uri) {
        final StringBuilder stringBuilder = new StringBuilder();
        if(uri.getScheme() != null) {
            stringBuilder.append(uri.getScheme())
                .append(":");
        }
        if(uri.getHost() != null) {
            stringBuilder.append("//")
                .append(uri.getHost());
            if(uri.getPort() >= 0) {
                stringBuilder.append(":")
                    .append(Integer.toString(uri.getPort()));
            }
        }
        stringBuilder.append(uri.getPath());
        return stringBuilder
            .toString();
    }

    public FileDecryptionProperties getFileDecryptionProperties() {
        return fileDecryptionProperties;
    }
    
    public Filter getRecordFilter() {
        return recordFilter;
    }


    public static Builder builder(final File file) {
        return new Builder(file.toURI()).tableName(file.getName());
    }

    public static Builder builder(final String inputPath) {
        return builder(URI.create(inputPath));
    }

    public static Builder builder(final URL url) {
        try {
            return builder(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Builder builder(final URI uri) {
        return new Builder(uri).tableName(sanitize(uri));
    }
    
    static Builder builderForStream() {
        return new Builder((URI)null);
    }

    public static class Builder extends ReadOptions.Builder {
        private boolean convertInt96ToTimestamp = false;
        private UnnanotatedBinaryAs unnanotatedBinaryAs = UnnanotatedBinaryAs.STRING;
        private ManageGroupsAs manageGroupsAs = ManageGroupsAs.TEXT;
        private String[] columns = new String[0];
        private final URI inputURI;
        private byte[] footerKey;
        private Map<String, byte[]> columnKeyMap;
        private byte[] aadPrefix;
        private DecryptionKeyRetriever keyRetriever;
        private boolean checkFooterIntegrity = true;
        private AADPrefixVerifier aadPrefixVerifier;
        private Filter recordFilter = FilterCompat.NOOP;

        protected Builder(final URI inputURI) {
            super();
            this.inputURI = inputURI;
        }

        @Override
        public TablesawParquetReadOptions build() {
            return new TablesawParquetReadOptions(this);
        }

        protected FileDecryptionProperties getFileDecryptionProperties() {
            if(footerKey == null && columnKeyMap == null && keyRetriever == null) {
                return null;
            }
            final FileDecryptionProperties.Builder fdpBuilder = FileDecryptionProperties.builder();
            if(footerKey != null) {
                fdpBuilder.withFooterKey(footerKey);
            }
            if(columnKeyMap != null) {
                final Map<ColumnPath, ColumnDecryptionProperties> columnProperties = new HashMap<>();
                for(Entry<String, byte[]> entry : columnKeyMap.entrySet()) {
                    columnProperties.put(ColumnPath.get(entry.getKey()),
                        ColumnDecryptionProperties.builder(entry.getKey()).withKey(entry.getValue()).build());
                }
                fdpBuilder.withColumnKeys(columnProperties);
            }
            if(aadPrefix != null) {
                fdpBuilder.withAADPrefix(aadPrefix);
            }
            if(keyRetriever != null) {
                fdpBuilder.withKeyRetriever(keyRetriever);
            }
            if(!checkFooterIntegrity) {
                fdpBuilder.withoutFooterSignatureVerification();
            }
            if(aadPrefixVerifier != null) {
                fdpBuilder.withAADPrefixVerifier(aadPrefixVerifier);
            }
            return fdpBuilder.build();
        }

        // Override super-class setters to return an instance of this class

        /** {@inheritDoc} This option is not used by TablesawParquetReadOptions */
        @Override
        public Builder header(final boolean header) {
            super.header(header);
            return this;
        }

        /**
         * Set the table name
         * @param tableName the table name
         * @return this for chaining
         */
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
            super.allowDuplicateColumnNames(allow);
            return this;
        }

        @Override
        public Builder skipRowsWithInvalidColumnCount(boolean skipRowsWithInvalidColumnCount) {
            super.skipRowsWithInvalidColumnCount(skipRowsWithInvalidColumnCount);
            return this;
        }

        /**
         * {@inheritDoc}
         * If used in conjunction with the {@link #withOnlyTheseColumns(String...)} options,
         * the provided ColumnType array must contain only the selected columns in the order they were provided.
         */
        @Override
        public Builder columnTypes(ColumnType[] columnTypes) {
            super.columnTypes(columnTypes);
            return this;
        }

        @Override
        public Builder columnTypes(Function<String, ColumnType> columnTypeFunction) {
            super.columnTypes(columnTypeFunction);
            return this;
        }

        @Override
        public Builder columnTypesPartial(Function<String, Optional<ColumnType>> columnTypeFunction) {
            super.columnTypesPartial(columnTypeFunction);
            return this;
        }

        @Override
        public Builder columnTypesPartial(Map<String, ColumnType> columnTypeByName) {
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
        
        /**
         * Read only a subset of columns, identified by name.
         * If used with the {@link #columnTypes(ColumnType[])} option, the ColumnType array
         * must contain only the selected columns in the order they were provided.
         * @param columns the column names to read
         * @return this builder
         */
        public Builder withOnlyTheseColumns(final String... columns) {
            this.columns = columns;
            return this;
        }
        
        /**
         * Set the footer key used for encryption
         * @param footerKey the footer key, must be either 16, 24 or 32 bytes.
         * @return this builder
         */
        public Builder withFooterKey(final byte[] footerKey) {
            this.footerKey = footerKey;
            return this;
        }
        
        /**
         * Sets the keys for column encryption.
         * @param columnKeyMap map column names to columns keys. Keys must be either 16, 24 or 32 bytes.
         * @return this builder
         */
        public Builder withColumnKeys(final Map<String, byte[]> columnKeyMap) {
            this.columnKeyMap = columnKeyMap;
            return this;
            
        }
        
        /**
         * Sets the AAD prefix. Required if it is not stored in the file.
         * @param aadPrefix the AADPrefix
         * @return this builder
         */
        public Builder withAADPrefix(final byte[] aadPrefix) {
            this.aadPrefix = aadPrefix;
            return this;
        }
        
        /**
         * Use a keyRetriever for fetching encryption keys
         * @param keyRetriever
         * @return this builder
         */
        public Builder withKeyRetriever(final DecryptionKeyRetriever keyRetriever) {
            this.keyRetriever = keyRetriever;
            return this;
        }
        
        /**
         * Do not check plain footer integrity in files without footer encryption
         * @return this builder
         */
        public Builder withoutFooterSignatureVerification() {
            this.checkFooterIntegrity = false;
            return this;
        }
        
        /**
         * Set callback for verification of AAD Prefixes stored in file.
         *
         * @param aadPrefixVerifier AAD prefix verification object
         * @return  this builder
         */
        public Builder withAADPrefixVerifier(final AADPrefixVerifier aadPrefixVerifier) {
            this.aadPrefixVerifier = aadPrefixVerifier;
            return this;
        }

        public Builder withRecordFilter(FilterPredicate rowGroupFilter) {
            this.recordFilter = FilterCompat.get(rowGroupFilter);
            return this;
          }
}

}
