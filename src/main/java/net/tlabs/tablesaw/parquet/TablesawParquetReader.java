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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.DataReader;
import tech.tablesaw.io.RuntimeIOException;
import tech.tablesaw.io.Source;

public class TablesawParquetReader implements DataReader<TablesawParquetReadOptions> {

    private static final Logger LOG = LoggerFactory.getLogger(TablesawParquetReader.class);

    public TablesawParquetReader() {
        super();
    }

    @Override
    public Table read(final Source source) {
        final File file = source.file();
        if (file != null) {
            return read(TablesawParquetReadOptions.builder(file).build());
        }
        final InputStream inStream = source.inputStream();
        if(inStream != null) {
            return readFromStream(inStream);
        }
        throw new UnsupportedOperationException("Reading parquet from a character stream is not supported");
    }

    @Override
    public Table read(final TablesawParquetReadOptions options) {
        final TablesawReadSupport readSupport = new TablesawReadSupport(options);
        try (final ParquetReader<Row> reader = makeReader(options.getInputURI(), readSupport, options)) {
            return readInternal(reader, readSupport, options.getSanitizedinputPath());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }
    
    private static Table readFromStream(final InputStream inStream) {
        final TablesawParquetReadOptions options = TablesawParquetReadOptions.builderForStream().build();
        final TablesawReadSupport readSupport = new TablesawReadSupport(options);
        try (final ParquetReader<Row> reader = makeReaderFromStream(inStream, readSupport, options)){
            return readInternal(reader, readSupport, "stream");
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private static Table readInternal(final ParquetReader<Row> reader, final TablesawReadSupport readSupport,
            final String displayName) throws IOException {
        final long start = System.currentTimeMillis();
        int i = 0;
        try {
            while (reader.read() != null) {
                i++;
            }
        } catch(Exception e) {
            LOG.error("Error reading row {}: {} - {}", i, e.getClass().getSimpleName(), e.getMessage());
            throw e;
        }
        final long end = System.currentTimeMillis();
        LOG.debug("Finished reading {} rows from {} in {} ms", i, displayName, (end - start));
        return readSupport.getTable();
    }
    
    private static ParquetReader<Row> makeReader(final URI uri, final TablesawReadSupport readSupport,
            final TablesawParquetReadOptions options) throws IOException {
        final String scheme = uri.getScheme();
        if(scheme != null) {
            switch(scheme) {
                case "http":   // fall through
                case "https":  // fall through
                case "ftp":    // fall through
                case "ftps":   // fall through
                    try(final InputStream inStream = uri.toURL().openStream()) {
                        return makeReaderFromStream(inStream, readSupport, options);
                    }
                default:
                    // fall through
            }
        }
        final Builder<Row> parquetReader = ParquetReader.builder(readSupport, new Path(uri))
            .withDecryption(options.getFileDecryptionProperties())
            .withFilter(options.getRecordFilter());
        
        return parquetReader.build();
    }

    private static ParquetReader<Row> makeReaderFromStream(final InputStream inStream, final TablesawReadSupport readSupport,
            final TablesawParquetReadOptions options) throws IOException {
        final File tmpFile = createSecureTempFile("tablesaw-parquet", "parquet");
        tmpFile.deleteOnExit();
        try(final FileOutputStream outStream = new FileOutputStream(tmpFile)) {
            IOUtils.copyLarge(inStream, outStream);
            return makeReader(tmpFile.toURI(), readSupport, options);
        }
    }
    
    private static File createSecureTempFile(final String prefix, final String suffix) throws IOException {
        if(SystemUtils.IS_OS_UNIX) {
            final FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rw-------"));
            return Files.createTempFile(prefix, suffix, attr).toFile();
        }
        final File tmpFile = Files.createTempFile(prefix, suffix).toFile();
        if(!(tmpFile.setReadable(true, true) && tmpFile.setWritable(true, true))) {
            throw new IOException("Unable to set required permission on temporary file " + tmpFile.getAbsolutePath());
        }
        return tmpFile;
    }
}
