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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;

abstract class AbstractTableParameterizedTest {

    protected static Table table;

    @ParameterizedTest
    @MethodSource("columnTypeParameters")
    void testColumnType(final int columnIndex, final ColumnType type, final String initialType) {
        assertEquals(type, table.column(columnIndex).type(),
            String.format("Wrong column type %s from %s at index %d", type.name(), initialType, columnIndex));
    }

    @ParameterizedTest
    @MethodSource("columnValueParameters")
    void testColumnValues(final int columnIndex, final int rowIndex, final Object value, final String type) {
        assertEquals(value, table.column(columnIndex).get(rowIndex),
            String.format("Wrong %s value at [%d,%d]", type, columnIndex, rowIndex));
    }
}
