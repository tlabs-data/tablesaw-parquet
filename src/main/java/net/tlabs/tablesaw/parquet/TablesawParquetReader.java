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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.DataReader;
import tech.tablesaw.io.Source;

public class TablesawParquetReader implements DataReader<TablesawParquetReadOptions> {

    private static final Logger LOG = LoggerFactory.getLogger(TablesawParquetReader.class);

    public TablesawParquetReader() {
        super();
    }

    @Override
    public Table read(final Source source) throws IOException {
        final File file = source.file();
        if (file != null) {
            return read(TablesawParquetReadOptions.builder(file).build());
        }
        throw new UnsupportedOperationException(
            "Can only work with file based source, please use the read(TablesawParquetReadOptions) method for additional possibilities");
    }

    @Override
    public Table read(final TablesawParquetReadOptions options) throws IOException {
        final long start = System.currentTimeMillis();
        final TablesawReadSupport readSupport = new TablesawReadSupport(options);
        try (final ParquetReader<Row> reader = makeReader(options.getInputURI(), readSupport)) {
            int i = 0;
            while (reader.read() != null) {
                i++;
            }
            final long end = System.currentTimeMillis();
            LOG.debug("Finished reading {} rows from {} in {} ms", i, options.getSanitizedinputPath(), (end - start));
        }
        return readSupport.getTable();
    }
    
    private ParquetReader<Row> makeReader(final URI uri, final TablesawReadSupport readSupport) throws IOException {
        final String scheme = uri.getScheme();
        if(scheme != null) {
            switch(scheme) {
                case "http":   // fall through
                case "https":  // fall through
                case "ftp":    // fall through
                case "ftps":   // fall through
                    try(final InputStream inStream = uri.toURL().openStream()) {
                        return makeReaderFromStream(readSupport, inStream);
                    }
                default:
                    // fall through
            }
        }
        return ParquetReader.builder(readSupport, new Path(uri)).build();
    }

    private ParquetReader<Row> makeReaderFromStream(final TablesawReadSupport readSupport, final InputStream inStream) throws IOException {
        final File tmpFile = File.createTempFile("tablesaw-parquet", "parquet");
        tmpFile.deleteOnExit();
        try(final FileOutputStream outStream = new FileOutputStream(tmpFile)) {
            IOUtils.copyLarge(inStream, outStream);
            return ParquetReader.builder(readSupport, new Path(tmpFile.toURI())).build();
        }
    }
}
