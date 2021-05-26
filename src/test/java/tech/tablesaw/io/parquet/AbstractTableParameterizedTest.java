package tech.tablesaw.io.parquet;

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
    assertEquals(
        type,
        table.column(columnIndex).type(),
        String.format(
            "Wrong column type %s from %s at index %d", type.name(), initialType, columnIndex));
  }

  @ParameterizedTest
  @MethodSource("columnValueParameters")
  void testColumnValues(
      final int columnIndex, final int rowIndex, final Object value, final String type) {
    assertEquals(
        value,
        table.column(columnIndex).get(rowIndex),
        String.format("Wrong %s value at [%d,%d]", type, columnIndex, rowIndex));
  }
}
