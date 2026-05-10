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
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Header.header;
import static org.mockserver.model.BinaryBody.binary;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpStatusCode;

import tech.tablesaw.api.Table;

class TestParquetReadFromHTTP {

    private static final int HTTP_PORT = 1080;
    private static final String HTTP_PATH = "/download/pandas_pyarrow.parquet";
    private static final String HTTP_PATH_NOEXT = "/download/pandas_pyarrow";
    private static final String FILE_URL = "http://localhost:" + HTTP_PORT + HTTP_PATH;
    private static final String FILE_URL_NOEXT = "http://localhost:" + HTTP_PORT + HTTP_PATH_NOEXT;
    private static ClientAndServer mockServer;

    @BeforeAll
    static void startServer() throws IOException {
        mockServer = startClientAndServer(HTTP_PORT);
        try(final InputStream in = new FileInputStream("target/test-classes/pandas_pyarrow.parquet")) {
            byte[] fileBytes = IOUtils.toByteArray(in);
            mockServer
            .when(
                request()
                    .withPath(HTTP_PATH)
            )
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withHeaders(
                        header("Content-Disposition", "inline"),
                        header("Content-Type", "binary/octet-stream")
                    )
                    .withBody(binary(fileBytes))
            );
            mockServer
            .when(
                request()
                    .withPath(HTTP_PATH_NOEXT)
            )
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withHeaders(
                        header("Content-Disposition", "inline"),
                        header("Content-Type", "application/vnd.apache.parquet")
                    )
                    .withBody(binary(fileBytes))
            );
        }
    }
 
    @AfterAll 
    static void stopServer() { 
        mockServer.stop();
    }
    
    @Test
    void testParquetFileFromHTTPUsingURL() throws MalformedURLException, URISyntaxException {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(new URI(FILE_URL).toURL()).build());
        assertNotNull(table, "Table is null");
    }

    @Test
    void testParquetFileFromHTTPUsingString() {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(FILE_URL).build());
        assertNotNull(table, "Table is null");
    }

    @Test
    void testParquetFileFromHTTPUsingURI() throws URISyntaxException {
        final Table table = new TablesawParquetReader().read(
            TablesawParquetReadOptions.builder(new URI(FILE_URL)).build());
        assertNotNull(table, "Table is null");
    }

    @Test
    void testParquetFileFromHTTPUsingMimeType() {
        TablesawParquet.registerReader();
        final Table table = Table.read().url(FILE_URL_NOEXT);
        assertNotNull(table, "Table is null");
    }
}
