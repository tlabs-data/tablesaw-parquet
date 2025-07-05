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

import tech.tablesaw.api.Table;

/**
 * Non instantiable Class containing static methods to register the parquet
 * reader and writer with Tablesaw
 */
public class TablesawParquet {

    private TablesawParquet() {
        super();
    }

    /**
     * Register the TablesawParquetReader and TablesawParquetWriter in the Tablesaw
     * registries.
     */
    public static void register() {
        registerReader();
        registerWriter();
    }

    /**
     * Register the TablesawParquetReader in the Tablesaw registry.
     * Reader is associated with its options and the '.parquet' extension.
     */
    public static void registerReader() {
        final TablesawParquetReader registeredInstance = new TablesawParquetReader();
        Table.defaultReaderRegistry.registerOptions(TablesawParquetReadOptions.class, registeredInstance);
        Table.defaultReaderRegistry.registerExtension("parquet", registeredInstance);
    }

    /**
     * Register the TablesawParquetWriter in the Tablesaw registry.
     * Writer is only associated with its options.
     */
    public static void registerWriter() {
        Table.defaultWriterRegistry.registerOptions(TablesawParquetWriteOptions.class, new TablesawParquetWriter());
    }

}
