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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import tech.tablesaw.api.Table;

class TestParquetReadFromFTP {

    private static final String PANDAS_PYARROW = "target/test-classes/pandas_pyarrow.parquet";

    private static FakeFtpServer fakeFtpServer;
    
    @BeforeAll
    static void setup() throws IOException {
        fakeFtpServer = new FakeFtpServer();
        fakeFtpServer.setServerControlPort(9021);
        final UserAccount userAccount = new UserAccount("user", "pwd", "/");
        fakeFtpServer.addUserAccount(userAccount);
        final FileSystem fileSystem = new UnixFakeFileSystem();
        try(final FileInputStream inputStream = new FileInputStream(PANDAS_PYARROW)) {
            final FileEntry entry = new FileEntry("/pandas_pyarrow.parquet");
            try(final OutputStream entryOutputStream = entry.createOutputStream(false)) {
                IOUtils.copy(inputStream, entryOutputStream);
                fileSystem.add(entry);
                fakeFtpServer.setFileSystem(fileSystem);
                fakeFtpServer.start();
            }
        }
    }
    
    @AfterAll
    static void tearDown() {
        fakeFtpServer.stop();
    }
    
    @Test
    void testReadParquetFromFTPUsingURL() throws MalformedURLException {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(new URL("ftp://user:pwd@localhost:9021/pandas_pyarrow.parquet")).build());
        assertNotNull(table, "Table is null");
        assertEquals("ftp://localhost:9021/pandas_pyarrow.parquet", table.name());
    }
    
    @Test
    void testReadParquetFromFTPUsingString() {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder("ftp://user:pwd@localhost:9021/pandas_pyarrow.parquet").build());
        assertNotNull(table, "Table is null");
        assertEquals("ftp://localhost:9021/pandas_pyarrow.parquet", table.name());
    }

    @Test
    void testReadParquetFromFTPUsingURI() throws URISyntaxException {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(new URI("ftp://user:pwd@localhost:9021/pandas_pyarrow.parquet")).build());
        assertNotNull(table, "Table is null");
        assertEquals("ftp://localhost:9021/pandas_pyarrow.parquet", table.name());
    }
    
}
