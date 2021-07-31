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
import java.io.IOException;
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
    final String inputPath = options.getInputPath();
    final Path path = new Path(inputPath);
    final TablesawReadSupport readSupport = new TablesawReadSupport(options);
    try (final ParquetReader<Row> reader = ParquetReader.<Row>builder(readSupport, path).build()) {
      int i = 0;
      while (reader.read() != null) {
        i++;
      }
      final long end = System.currentTimeMillis();
      LOG.debug("Finished reading {} rows from {} in {} ms", i, inputPath, (end - start));
    }
    return readSupport.getTable();
  }
}
