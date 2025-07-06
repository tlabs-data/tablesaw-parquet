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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import tech.tablesaw.io.WriteOptions;

/**
 * Options for writing tablesaw table in parquet.
 * Use the static {@code builder} methods
 */
public class TablesawParquetWriteOptions extends WriteOptions {

    public enum CompressionCodec {
        UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4
    }

    private final String outputFile;
    private final CompressionCodec compressionCodec;
    private final boolean overwrite;
    private final boolean writeChecksum;
    private final FileEncryptionProperties fileEncryptionProperties;

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
        this.writeChecksum = builder.writeChecksum;
        this.fileEncryptionProperties = builder.getEncryptionProperties();
    }

    public String getOutputFile() {
        return outputFile;
    }

    public CompressionCodec getCompressionCodec() {
        return compressionCodec;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public boolean isWriteChecksum() {
        return writeChecksum;
    }

    public FileEncryptionProperties getFileEncryptionProperties() {
        return fileEncryptionProperties;
    }

    public static class Builder extends WriteOptions.Builder {

        private final String outputFile;
        private CompressionCodec compressionCodec = CompressionCodec.SNAPPY;
        private boolean overwrite = true;
        private boolean writeChecksum = false;
        private byte[] footerKeyBytes;
        private ParquetCipher parquetCipher;
        private byte[] aadPrefix;
        private Map<String, byte[]> columnKeyMap;
        private boolean storeAadPrefixInFile = true;
        private boolean completeColumnEncryption = false;
        private boolean encryptedFooter = true;
        private byte[] footerKeyMetadata;
        private Map<String, byte[]> columnMetadataMap;

        public Builder(final String outputFile) {
            super((Writer) null);
            this.outputFile = outputFile;
        }

        protected FileEncryptionProperties getEncryptionProperties() {
            if(footerKeyBytes == null) return null;
            final FileEncryptionProperties.Builder fileEncryptionPropertiesBuilder = 
                FileEncryptionProperties.builder(footerKeyBytes);
            if(footerKeyMetadata != null) {
                fileEncryptionPropertiesBuilder.withFooterKeyMetadata(footerKeyMetadata);
            }
            if(parquetCipher != null) {
                fileEncryptionPropertiesBuilder.withAlgorithm(parquetCipher);
            }
            if(aadPrefix != null) {
                fileEncryptionPropertiesBuilder.withAADPrefix(aadPrefix);
                if(!storeAadPrefixInFile) {
                    fileEncryptionPropertiesBuilder.withoutAADPrefixStorage();
                }
            }
            if(columnKeyMap != null) {
                final Map<ColumnPath, ColumnEncryptionProperties> columnProperties = new HashMap<>();
                for(Entry<String, byte[]> entry : columnKeyMap.entrySet()) {
                    final ColumnEncryptionProperties.Builder columnEncryptionProperties =
                        ColumnEncryptionProperties.builder(entry.getKey()).withKey(entry.getValue());
                    if(columnMetadataMap != null) {
                        columnEncryptionProperties.withKeyMetaData(columnMetadataMap.get(entry.getKey()));
                    }
                    columnProperties.put(ColumnPath.get(entry.getKey()), columnEncryptionProperties.build());
                }
                fileEncryptionPropertiesBuilder.withEncryptedColumns(columnProperties);
            }
            if(completeColumnEncryption) {
                fileEncryptionPropertiesBuilder.withCompleteColumnEncryption();
            }
            if(!encryptedFooter) {
                fileEncryptionPropertiesBuilder.withPlaintextFooter();
            }
            return fileEncryptionPropertiesBuilder.build();
        }
        
        /**
         * Sets the footer encryption key. Mandatory for all encryptions setup.
         * @param footerKeyBytes the footer key, must be either 16, 24 or 32 bytes.
         * @return this builder
         */
        public Builder withEncryption(final byte[] footerKeyBytes) {
            this.footerKeyBytes = footerKeyBytes;
            return this;
        }

        /**
         * Sets the cipher to use for encryption
         * @param parquetCipher the cipher
         * @return this builder
         */
        public Builder withCipher(final ParquetCipher parquetCipher) {
            this.parquetCipher = parquetCipher;
            return this;
        }

        /**
         * Sets the AAD prefix for encryption.
         * @param aadPrefix the ADDPrefix
         * @return this builder
         */
        public Builder withAADdPrefix(final byte[] aadPrefix) {
            this.aadPrefix = aadPrefix;
            return this;
        }
        
        /*
         * Do not store the AADPrefix in the file.
         * If not store in the file, the AADPrefix must be provided for decryption.
         */
        public Builder withoutAADPrefixStorage() {
            this.storeAadPrefixInFile = false;
            return this;
        }

        /**
         * If some column keys are provided, columns with no provided keys will be encrypted with the footer key.
         * @return
         */
        public Builder withCompleteColumnEncryption() {
            this.completeColumnEncryption = true;
            return this;
        }

        /**
         * Set column specific keys. Columns in the map will be encrypted with their specific keys.
         * Other columns will not be encrypted except if {@link net.tlabs.tablesaw.parquet.TablesawParquetWriteOptions.Builder#withCompleteColumnEncryption()}
         * is used. In this case columns not in the map will be encrypted using the footer key.
         * @param columnKeyMap the map of column names to keys.
         * @return this builder
         */
        public Builder withEncryptedColumns(final Map<String, byte[]> columnKeyMap) {
            this.columnKeyMap = columnKeyMap;
            return this;
        }

        /**
         * Sets the compression code to use.
         * @see net.tlabs.tablesaw.parquet.TablesawParquetWriteOptions.CompressionCodec
         * @param compressionCodec the compression codec
         * @return this builder
         */
        public Builder withCompressionCode(final CompressionCodec compressionCodec) {
            this.compressionCodec = compressionCodec;
            return this;
        }

        /**
         * Allows the writer to override existing files. Default is false.
         * @param overwrite sets whether the writer can override existing files. Default is false
         * @return this builder
         */
        public Builder withOverwrite(final boolean overwrite) {
            this.overwrite = overwrite;
            return this;
        }

        /**
         * Sets whether to write checksum file along with the data file. Default is false.
         * @param writeChecksum sets whether to write checksum file along with the data file. Default is false.
         * @return this builder
         */
        public Builder withWriteChecksum(final boolean writeChecksum) {
            this.writeChecksum = writeChecksum;
            return this;
        }

        /**
         * Create files with plaintext footer.
         * If not called, the files will be created with encrypted footer (default).
         * @return this builder
         */
        public Builder withPlainTextFooter() {
            this.encryptedFooter = false;
            return this;
        }
        
        /**
         * Sets the footer key metadata for key retriever
         * @param footerKeyMetadata the footer key metadata as bytes
         * @return
         */
        public Builder withFooterKeyMetadata(final byte[] footerKeyMetadata) {
            this.footerKeyMetadata = footerKeyMetadata;
            return this;
        }
        
        /**
         * Sets the footer key metadata for key retriever
         * @param footerKeyID the footer key metadata as string
         * @return
         */
        public Builder withFooterKeyID(final String footerKeyId) {
            this.footerKeyMetadata = footerKeyId.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        /**
         * Sets the encrypted columns metadata
         * @param columnMetadataMap map column names to column metadata as bytes
         * @return this builder
         */
        public Builder withEncryptedColumnMetadata(final Map<String, byte[]> columnMetadataMap) {
            this.columnMetadataMap = columnMetadataMap;
            return this;
        }

        /**
         * Sets the encrypted columns metadata
         * @param columnMetadataMap map column names to column metadata as strings
         * @return this builder
         */
        public Builder withEncryptedColumnID(final Map<String, String> columnIDMap) {
            this.columnMetadataMap = new HashMap<>();
            for(Map.Entry<String, String> entry : columnIDMap.entrySet()) {
                this.columnMetadataMap.put(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
            }
            return this;
        }

        /**
         * Build the {@link net.tlabs.tablesaw.parquet.TablesawParquetWriteOptions}
         * @return the options
         */
        public TablesawParquetWriteOptions build() {
            return new TablesawParquetWriteOptions(this);
        }

    }
}
