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

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;
import tech.tablesaw.api.ColumnType;

class TestMinimizedReadPyArrow extends AbstractTableParameterizedTest {

    @SuppressWarnings("unused")
    private static Stream<Arguments> columnTypeParameters() {
        return Stream.of(Arguments.of(0, ColumnType.BOOLEAN, "boolean"), Arguments.of(1, ColumnType.SHORT, "uint8"),
            Arguments.of(2, ColumnType.SHORT, "int8"), Arguments.of(3, ColumnType.SHORT, "uint16"),
            Arguments.of(4, ColumnType.SHORT, "int16"), Arguments.of(5, ColumnType.INTEGER, "uint32"),
            Arguments.of(6, ColumnType.INTEGER, "int32"), Arguments.of(7, ColumnType.LONG, "uint64"),
            Arguments.of(8, ColumnType.LONG, "int64"), Arguments.of(9, ColumnType.FLOAT, "float32"),
            Arguments.of(10, ColumnType.DOUBLE, "float64"),
            Arguments.of(11, ColumnType.LOCAL_DATE_TIME, "TIMESTAMP_MILLIS"),
            Arguments.of(12, ColumnType.STRING, "string"));
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> columnValueParameters() {
        return Stream.of(Arguments.of(0, 0, true, "boolean"), Arguments.of(0, 1, false, "boolean"),
            Arguments.of(1, 0, (short) 0, "short"), Arguments.of(1, 1, (short) 127, "short"),
            Arguments.of(2, 0, (short) -127, "short"), Arguments.of(2, 1, (short) 1, "short"),
            Arguments.of(3, 0, (short) 0, "short"), Arguments.of(3, 1, (short) 32767, "short"),
            Arguments.of(4, 0, (short) 0, "short"), Arguments.of(4, 1, (short) -32767, "short"),
            Arguments.of(5, 0, 0, "integer"), Arguments.of(5, 1, 65000, "integer"), Arguments.of(6, 0, 0, "integer"),
            Arguments.of(6, 1, -65000, "integer"), Arguments.of(7, 0, 0L, "long"),
            Arguments.of(7, 1, 1_000_000_000L, "long"), Arguments.of(8, 0, 0L, "long"),
            Arguments.of(8, 1, -1_000_000_000L, "long"), Arguments.of(9, 0, null, "float"),
            Arguments.of(9, 1, 1.0f, "float"), Arguments.of(10, 0, 0.0d, "double"), Arguments.of(10, 1, null, "double"),
            Arguments.of(11, 0, LocalDateTime.of(2021, 4, 23, 0, 0), "LocalDateTime"),
            Arguments.of(11, 1, LocalDateTime.of(2021, 4, 23, 0, 0, 1), "LocalDateTime"),
            Arguments.of(12, 0, "string1", "String"), Arguments.of(12, 1, "string2", "String"));
    }

    private static final String PANDAS_PYARROW = "target/test-classes/pandas_pyarrow.parquet";

    @BeforeAll
    static void beforeAll() throws IOException {
        table = TablesawParquetReader.getInstance()
            .read(TablesawParquetReadOptions.builder(new File(PANDAS_PYARROW)).minimizeColumnSizes().build());
        assertEquals("pandas_pyarrow.parquet", table.name(), "Wrong table name");
    }
}
