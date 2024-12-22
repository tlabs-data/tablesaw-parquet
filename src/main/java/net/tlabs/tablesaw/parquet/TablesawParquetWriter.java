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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.DataWriter;
import tech.tablesaw.io.Destination;
import tech.tablesaw.io.RuntimeIOException;

public class TablesawParquetWriter implements DataWriter<TablesawParquetWriteOptions> {

    private static final Logger LOG = LoggerFactory.getLogger(TablesawParquetWriter.class);

    public TablesawParquetWriter() {
        super();
    }

    @Override
    public void write(final Table table, final Destination dest) {
        throw new UnsupportedOperationException(
            "The use of Destination is not supported, please use the write(Table, TablesawParquetWriteOptions) method");
    }

    @Override
    public void write(final Table table, final TablesawParquetWriteOptions options) {
        
        try {
            final FileSystem fs = FileSystem.getLocal(new Configuration());
            fs.setWriteChecksum(options.isWriteChecksum());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }

        try (final ParquetWriter<Row> writer = new Builder(new Path(options.getOutputFile()), table)
                .withCompressionCodec(CompressionCodecName.fromConf(options.getCompressionCodec().name()))
                .withWriteMode(options.isOverwrite() ? Mode.OVERWRITE : Mode.CREATE).withValidation(false)
                .build()) {
            final long start = System.currentTimeMillis();
            for(final Row row : table) {
                writer.write(row);
            }
            final long end = System.currentTimeMillis();
            LOG.debug("Finished writing {} rows to {} in {} ms",
                table.rowCount(), options.getOutputFile(), (end - start));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }
    
    protected static class Builder extends ParquetWriter.Builder<Row, Builder> {

        private final Table table;

        protected Builder(final Path path, final Table table) {
            super(path);
            this.table = table;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<Row> getWriteSupport(final Configuration conf) {
            return new TablesawWriteSupport(this.table);
        }
    }
}
