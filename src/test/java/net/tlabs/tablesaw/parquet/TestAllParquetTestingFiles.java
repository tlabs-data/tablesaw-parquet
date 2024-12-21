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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import tech.tablesaw.api.Table;

class TestAllParquetTestingFiles {

    private static final String PARQUET_TESTING_FOLDER = "target/test/data/parquet-testing-master/data/";

    private static Stream<Arguments> listParquetTestingFile() throws IOException {
        return Files.list(Paths.get(PARQUET_TESTING_FOLDER))
            .map(Path::toFile)
            .filter(TestAllParquetTestingFiles::filterPath)
            .map(f -> Arguments.of(f));
    }
    
    private static boolean filterPath(final File file) {
        if (!file.isFile()) return false;
        final String filename = file.getName();
        // keep only unencrypted parquet files
        if (!filename.endsWith(".parquet")) return false;
        // lz4 not supported
        if(filename.contains("lz4")) return false;
        // brotli not supported
        if(filename.contains("brotli")) return false;
        // parquet-mr reader fails on these ones
        if(filename.equals("nation.dict-malformed.parquet")) return false;
        if(filename.equals("fixed_length_byte_array.parquet")) return false;
        if(filename.equals("repeated_primitive_no_list.parquet")) return false;
        return true;
    }
    
    @ParameterizedTest
    @MethodSource("listParquetTestingFile")
    void testParquetTestingFile(final File parquetFile) throws IOException {
        final Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(parquetFile).build());
        assertNotNull(table);
    }
}
