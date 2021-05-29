package net.tlabs.tablesaw.parquet;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import tech.tablesaw.api.BooleanColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.ShortColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.api.TextColumn;
import tech.tablesaw.api.TimeColumn;
import tech.tablesaw.columns.Column;
import tech.tablesaw.columns.dates.PackedLocalDate;
import tech.tablesaw.columns.times.PackedLocalTime;

final class TableProxy {

  private final Table table;

  private final boolean[] rowColumnsSet;
  private final BooleanColumn[] booleanColumns;
  private final ShortColumn[] shortColumns;
  private final IntColumn[] intColumns;
  private final LongColumn[] longColumns;
  private final FloatColumn[] floatColumns;
  private final DoubleColumn[] doubleColumns;
  private final DateColumn[] dateColumns;
  private final TimeColumn[] timeColumns;
  private final DateTimeColumn[] dateTimeColumns;
  private final InstantColumn[] instantColumns;
  private final StringColumn[] stringColumns;
  private final TextColumn[] textColumns;

  private Row currentRow = null;
  private int currentRownum = -1;

  TableProxy(final Table table) {
    super();
    this.table = table;
    final List<Column<?>> columns = table.columns();
    final int size = columns.size();
    rowColumnsSet = new boolean[size];
    booleanColumns = new BooleanColumn[size];
    shortColumns = new ShortColumn[size];
    intColumns = new IntColumn[size];
    longColumns = new LongColumn[size];
    floatColumns = new FloatColumn[size];
    doubleColumns = new DoubleColumn[size];
    dateColumns = new DateColumn[size];
    timeColumns = new TimeColumn[size];
    dateTimeColumns = new DateTimeColumn[size];
    instantColumns = new InstantColumn[size];
    stringColumns = new StringColumn[size];
    textColumns = new TextColumn[size];
    for (int i = 0; i < size; i++) {
      fillColumnArrays(i, columns.get(i).type());
    }
  }

  private void fillColumnArrays(final int colIndex, final ColumnType columnType) {
    if (columnType == ColumnType.BOOLEAN) {
      booleanColumns[colIndex] = table.booleanColumn(colIndex);
    } else if (columnType == ColumnType.SHORT) {
      shortColumns[colIndex] = table.shortColumn(colIndex);
    } else if (columnType == ColumnType.INTEGER) {
      intColumns[colIndex] = table.intColumn(colIndex);
    } else if (columnType == ColumnType.LONG) {
      longColumns[colIndex] = table.longColumn(colIndex);
    } else if (columnType == ColumnType.FLOAT) {
      floatColumns[colIndex] = table.floatColumn(colIndex);
    } else if (columnType == ColumnType.DOUBLE) {
      doubleColumns[colIndex] = table.doubleColumn(colIndex);
    } else if (columnType == ColumnType.LOCAL_TIME) {
      timeColumns[colIndex] = table.timeColumn(colIndex);
    } else if (columnType == ColumnType.LOCAL_DATE) {
      dateColumns[colIndex] = table.dateColumn(colIndex);
    } else if (columnType == ColumnType.LOCAL_DATE_TIME) {
      dateTimeColumns[colIndex] = table.dateTimeColumn(colIndex);
    } else if (columnType == ColumnType.INSTANT) {
      instantColumns[colIndex] = table.instantColumn(colIndex);
    } else if (columnType == ColumnType.STRING) {
      stringColumns[colIndex] = table.stringColumn(colIndex);
    } else if (columnType == ColumnType.TEXT) {
      textColumns[colIndex] = table.textColumn(colIndex);
    }
  }

  void appendInstant(final int colIndex, final Instant value) {
    instantColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendDateTime(final int colIndex, final LocalDateTime value) {
    dateTimeColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendFloat(final int colIndex, final float value) {
    floatColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendDouble(final int colIndex, final double value) {
    doubleColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendTime(final int colIndex, final LocalTime value) {
    timeColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendString(final int colIndex, final String value) {
    stringColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendText(final int colIndex, final String value) {
    textColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendBoolean(final int colIndex, final boolean value) {
    booleanColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendShort(final int colIndex, final short value) {
    shortColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendInt(final int colIndex, final int value) {
    intColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendLong(final int colIndex, final long value) {
    longColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void appendDate(final int colIndex, final LocalDate value) {
    dateColumns[colIndex].append(value);
    rowColumnsSet[colIndex] = true;
  }

  void startRow() {
    currentRownum++;
  }

  void endRow() {
    for (int i = 0; i < rowColumnsSet.length; i++) {
      if (!rowColumnsSet[i]) {
        table.column(i).appendMissing();
      } else {
        rowColumnsSet[i] = false;
      }
    }
  }

  Row getCurrentRow() {
    if (this.currentRow == null) {
      this.currentRow = table.row(currentRownum);
    } else {
      currentRow.at(currentRownum);
    }
    return currentRow;
  }

  boolean getBoolean(final int colIndex, final int rowIndex) {
    return booleanColumns[colIndex].get(rowIndex);
  }

  int getShort(final int colIndex, final int rowIndex) {
    return shortColumns[colIndex].getShort(rowIndex);
  }

  int getInt(final int colIndex, final int rowIndex) {
    return intColumns[colIndex].getInt(rowIndex);
  }

  long getLong(final int colIndex, final int rowIndex) {
    return longColumns[colIndex].getLong(rowIndex);
  }

  float getFloat(final int colIndex, final int rowIndex) {
    return floatColumns[colIndex].getFloat(rowIndex);
  }

  double getDouble(final int colIndex, final int rowIndex) {
    return doubleColumns[colIndex].getDouble(rowIndex);
  }

  String getString(final int colIndex, final int rowIndex) {
    return stringColumns[colIndex].get(rowIndex);
  }

  String getText(final int colIndex, final int rowIndex) {
    return textColumns[colIndex].get(rowIndex);
  }

  int getDateToEpochDay(final int colIndex, final int rowIndex) {
    return (int) PackedLocalDate.toEpochDay(dateColumns[colIndex].getIntInternal(rowIndex));
  }

  long getTimeToNanoOfDay(final int colIndex, final int rowIndex) {
    return PackedLocalTime.toNanoOfDay(timeColumns[colIndex].getIntInternal(rowIndex));
  }

  long getInstantToEpochMilli(final int colIndex, final int rowIndex) {
    return instantColumns[colIndex].get(rowIndex).toEpochMilli();
  }

  long getDateTimeToEpochMilli(final int colIndex, final int rowIndex) {
    return dateTimeColumns[colIndex].get(rowIndex).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  Column<?> column(final int colIndex) {
    return table.column(colIndex);
  }
}
