package net.tlabs.tablesaw.parquet;

/*-
 * #%L
 * Tablesaw-Parquet
 * %%
 * Copyright (C) 2020 - 2022 Tlabs-data
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions.Builder;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;

class TestAllParquetTestingFiles {

    private static final String PARQUET_TESTING_FOLDER = "target/test/data/parquet-testing-master/data/";
    private static final byte[] FOOTER_ENCRYPTION_KEY = new String("0123456789012345").getBytes();
    private static final byte[] COLUMN_ENCRYPTION_KEY1 = new String("1234567890123450").getBytes();
    private static final byte[] COLUMN_ENCRYPTION_KEY2 = new String("1234567890123451").getBytes();
    private static final Map<String, byte[]> COLUMN_KEY_MAP = Map.of(
        "double_field", COLUMN_ENCRYPTION_KEY1,
        "float_field", COLUMN_ENCRYPTION_KEY2);
    private static final StringKeyIdRetriever KEY_RETRIEVER = new StringKeyIdRetriever();
    private static final String AAD_KEY = "tester";
    // AES256 keys
    private static final byte[] AES256_FOOTER_ENCRYPTION_KEY = new String("01234567890123456789012345678901").getBytes();
    private static final byte[] AES256_ENCRYPTION_KEY1 = new String("12345678901234567890123456789012").getBytes();
    private static final byte[] AES256_ENCRYPTION_KEY2 = new String("12345678901234567890123456789013").getBytes();
    private static final byte[] AES256_ENCRYPTION_KEY3 = new String("12345678901234567890123456789014").getBytes();
    private static final byte[] AES256_ENCRYPTION_KEY4 = new String("12345678901234567890123456789015").getBytes();
    private static final byte[] AES256_ENCRYPTION_KEY5 = new String("12345678901234567890123456789016").getBytes();
    private static final byte[] AES256_ENCRYPTION_KEY6 = new String("12345678901234567890123456789017").getBytes();
    private static final byte[] AES256_ENCRYPTION_KEY7 = new String("12345678901234567890123456789018").getBytes();
    private static final byte[] AES256_ENCRYPTION_KEY8 = new String("12345678901234567890123456789019").getBytes();
    private static final Map<String, byte[]> AES256_COLUMN_KEY_MAP = Map.of(
        "double_field", AES256_ENCRYPTION_KEY1,
        "float_field", AES256_ENCRYPTION_KEY2,
        "boolean_field", AES256_ENCRYPTION_KEY3,
        "int32_field", AES256_ENCRYPTION_KEY4,
        "ba_field", AES256_ENCRYPTION_KEY5,
        "flba_field", AES256_ENCRYPTION_KEY6,
        "int64_field.list.element", AES256_ENCRYPTION_KEY7,
        "int96_field", AES256_ENCRYPTION_KEY8);
    private static final StringKeyIdRetriever AES256_KEY_RETRIEVER = new StringKeyIdRetriever();
    
    static {
        KEY_RETRIEVER.putKey("kf", FOOTER_ENCRYPTION_KEY);
        KEY_RETRIEVER.putKey("kc1", COLUMN_ENCRYPTION_KEY1);
        KEY_RETRIEVER.putKey("kc2", COLUMN_ENCRYPTION_KEY2);
        // AES256 keys
        AES256_KEY_RETRIEVER.putKey("kf", AES256_FOOTER_ENCRYPTION_KEY);
        AES256_KEY_RETRIEVER.putKey("kc1", AES256_ENCRYPTION_KEY1);
        AES256_KEY_RETRIEVER.putKey("kc2", AES256_ENCRYPTION_KEY2);
        AES256_KEY_RETRIEVER.putKey("kc3", AES256_ENCRYPTION_KEY3);
        AES256_KEY_RETRIEVER.putKey("kc4", AES256_ENCRYPTION_KEY4);
        AES256_KEY_RETRIEVER.putKey("kc5", AES256_ENCRYPTION_KEY5);
        AES256_KEY_RETRIEVER.putKey("kc6", AES256_ENCRYPTION_KEY6);
        AES256_KEY_RETRIEVER.putKey("kc7", AES256_ENCRYPTION_KEY7);
        AES256_KEY_RETRIEVER.putKey("kc8", AES256_ENCRYPTION_KEY8);
    }

    private static Stream<Arguments> listParquetTestingFile() throws IOException {
        return Files.list(Paths.get(PARQUET_TESTING_FOLDER))
            .map(Path::toFile)
            .filter(TestAllParquetTestingFiles::filterUnencryptedParquet)
            .map(Arguments::of);
    }
    
    private static boolean filterUnencryptedParquet(final File file) {
        if (!file.isFile()) return false;
        final String filename = file.getName();
        // keep only unencrypted parquet files
        if (!filename.endsWith(".parquet")) return false;
        // brotli not supported
        if(filename.contains("brotli")) return false;
        // identified as LZ4 instead of LZ4_RAW
        // see https://github.com/apache/parquet-testing/pull/14
        if(filename.equals("non_hadoop_lz4_compressed.parquet")) return false;
        // parquet-java reader fails on this one
        // see https://github.com/apache/parquet-java/issues/3336
        if(filename.equals("nation.dict-malformed.parquet")) return false;
        // null logicalType
        // see https://github.com/apache/parquet-java/issues/2709
        if(filename.equals("unknown-logical-type.parquet")) return false;
        return true;
    }
    
    @ParameterizedTest
    @MethodSource("listParquetTestingFile")
    void testParquetTestingFile(final File parquetFile) {
        final Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(parquetFile).build());
        assertNotNull(table);
    }
    
    private static Stream<Arguments> listEncryptedTestingFile() throws IOException {
        return Files.list(Paths.get(PARQUET_TESTING_FOLDER))
            .map(Path::toFile)
            .filter(TestAllParquetTestingFiles::filterEncryptedParquet)
            .map(Arguments::of);
    }
    
    private static boolean filterEncryptedParquet(final File file) {
        if (!file.isFile()) return false;
        final String filename = file.getName();
        // keep only encrypted parquet files
        if (!filename.endsWith(".parquet.encrypted")) return false;
        // skip external key material
        if(filename.equals("external_key_material_java.parquet.encrypted")) return false;
        return true;
    }

    @ParameterizedTest
    @MethodSource("listEncryptedTestingFile")
    void testEncryptedTestingFile(final File parquetFile) {
        final Builder optionBuilder = TablesawParquetReadOptions.builder(parquetFile)
            .withFooterKey(FOOTER_ENCRYPTION_KEY)
            .withColumnKeys(COLUMN_KEY_MAP);
        if(parquetFile.getName().contains("_aad_")) {
            optionBuilder.withAADPrefix(AAD_KEY.getBytes());
        }
        final Table table = new TablesawParquetReader().read(optionBuilder.build());
        assertNotNull(table);
    }

    @ParameterizedTest
    @MethodSource("listEncryptedTestingFile")
    void testEncryptedTestingFileWithRetriever(final File parquetFile) {
        final Builder optionBuilder = TablesawParquetReadOptions
            .builder(parquetFile)
            .withKeyRetriever(KEY_RETRIEVER);
        if(parquetFile.getName().contains("_aad_")) {
            optionBuilder.withAADPrefix(AAD_KEY.getBytes());
        }
        final Table table = new TablesawParquetReader().read(optionBuilder.build());
        assertNotNull(table);
    }
    
    private static Stream<Arguments> listGeospatialFile() throws IOException {
        return Files.list(Paths.get(PARQUET_TESTING_FOLDER + "geospatial/"))
            .map(Path::toFile)
            .filter(TestAllParquetTestingFiles::filterGeospatialdParquet)
            .map(Arguments::of);
    }

    private static boolean filterGeospatialdParquet(final File file) {
        if (!file.isFile()) return false;
        final String filename = file.getName();
        // keep only parquet files
        if (!filename.endsWith(".parquet")) return false;
        return true;
    }

    @ParameterizedTest
    @MethodSource("listGeospatialFile")
    void testGeospatialFiles(final File parquetFile) {
        final Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(parquetFile).build());
        assertNotNull(table);
        final int columnCount = table.columnCount();
        assertEquals(ColumnType.STRING, table.column(columnCount - 1).type());
    }
    
    private static Stream<Arguments> listAES256TestingFile() throws IOException {
        return Files.list(Paths.get(PARQUET_TESTING_FOLDER + "aes256/"))
            .map(Path::toFile)
            .filter(TestAllParquetTestingFiles::filterAES256Parquet)
            .map(Arguments::of);
    }
    
    private static boolean filterAES256Parquet(final File file) {
        if (!file.isFile()) return false;
        final String filename = file.getName();
        // keep only encrypted parquet files
        if (!filename.endsWith(".parquet.encrypted")) return false;
        return true;
    }

    @ParameterizedTest
    @MethodSource("listAES256TestingFile")
    void testAES256TestingFile(final File parquetFile) {
        final Builder optionBuilder = TablesawParquetReadOptions
            .builder(parquetFile)
            .withFooterKey(AES256_FOOTER_ENCRYPTION_KEY);
        if(!parquetFile.getName().contains("uniform")) {
            optionBuilder.withColumnKeys(AES256_COLUMN_KEY_MAP);
        }
        if(parquetFile.getName().contains("disable_aad_storage")) {
            optionBuilder.withAADPrefix(AAD_KEY.getBytes());
        }
        final Table table = new TablesawParquetReader().read(optionBuilder.build());
        assertNotNull(table);
    }

    @ParameterizedTest
    @MethodSource("listAES256TestingFile")
    void testAES256TestingFileWithRetriever(final File parquetFile) {
        final Builder optionBuilder = TablesawParquetReadOptions
            .builder(parquetFile)
            .withKeyRetriever(AES256_KEY_RETRIEVER);
        if(parquetFile.getName().contains("disable_aad_storage")) {
            optionBuilder.withAADPrefix(AAD_KEY.getBytes());
        }
        final Table table = new TablesawParquetReader().read(optionBuilder.build());
        assertNotNull(table);
    }
}
