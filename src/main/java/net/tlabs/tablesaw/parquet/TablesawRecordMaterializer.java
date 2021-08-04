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

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

public class TablesawRecordMaterializer extends RecordMaterializer<Row> {

    private final TablesawRecordConverter root;

    public TablesawRecordMaterializer(final Table table, final MessageType fileSchema,
            final TablesawParquetReadOptions options) {
        super();
        this.root = new TablesawRecordConverter(table, fileSchema, options);
    }

    @Override
    public Row getCurrentRecord() {
        return root.getCurrentRow();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
