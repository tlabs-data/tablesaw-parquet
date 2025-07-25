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

    private Row currentRow = null;
    private int currentRownum;
    private boolean rowSkipped = false;

    TableProxy(final Table table) {
        super();
        this.table = table;
        this.currentRownum = table.rowCount() - 1;
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
        for (int i = 0; i < size; i++) {
            fillColumnArrays(i, columns.get(i).type());
        }
    }

    private void fillColumnArrays(final int colIndex, final ColumnType columnType) {
        if (ColumnType.BOOLEAN.equals(columnType)) {
            booleanColumns[colIndex] = table.booleanColumn(colIndex);
        } else if (ColumnType.SHORT.equals(columnType)) {
            shortColumns[colIndex] = table.shortColumn(colIndex);
        } else if (ColumnType.INTEGER.equals(columnType)) {
            intColumns[colIndex] = table.intColumn(colIndex);
        } else if (ColumnType.LONG.equals(columnType)) {
            longColumns[colIndex] = table.longColumn(colIndex);
        } else if (ColumnType.FLOAT.equals(columnType)) {
            floatColumns[colIndex] = table.floatColumn(colIndex);
        } else if (ColumnType.DOUBLE.equals(columnType)) {
            doubleColumns[colIndex] = table.doubleColumn(colIndex);
        } else if (ColumnType.LOCAL_TIME.equals(columnType)) {
            timeColumns[colIndex] = table.timeColumn(colIndex);
        } else if (ColumnType.LOCAL_DATE.equals(columnType)) {
            dateColumns[colIndex] = table.dateColumn(colIndex);
        } else if (ColumnType.LOCAL_DATE_TIME.equals(columnType)) {
            dateTimeColumns[colIndex] = table.dateTimeColumn(colIndex);
        } else if (ColumnType.INSTANT.equals(columnType)) {
            instantColumns[colIndex] = table.instantColumn(colIndex);
        } else if (ColumnType.STRING.equals(columnType)) {
            stringColumns[colIndex] = table.stringColumn(colIndex);
        } else {
            throw new IllegalArgumentException("Unsupported ColumnType " + columnType);
        }
    }

    void appendInstant(final int colIndex, final Instant value) {
        if(rowSkipped) {
            instantColumns[colIndex].set(currentRownum, value);            
        } else {
            instantColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendDateTime(final int colIndex, final LocalDateTime value) {
        if(rowSkipped) {
            dateTimeColumns[colIndex].set(currentRownum, value);            
        } else {
            dateTimeColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendFloat(final int colIndex, final float value) {
        if(rowSkipped) {
            floatColumns[colIndex].set(currentRownum, value);            
        } else {
            floatColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendDouble(final int colIndex, final double value) {
        if(rowSkipped) {
            doubleColumns[colIndex].set(currentRownum, value);            
        } else {
            doubleColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendTime(final int colIndex, final LocalTime value) {
        if(rowSkipped) {
            timeColumns[colIndex].set(currentRownum, value);            
        } else {
            timeColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendString(final int colIndex, final String value) {
        if(rowSkipped) {
            stringColumns[colIndex].set(currentRownum, value);            
        } else {
            stringColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendRepeatedString(final int colIndex, final String value) {
        if(rowColumnsSet[colIndex]) {
            final String initialContent = getString(colIndex, currentRownum);
            stringColumns[colIndex].set(currentRownum, "[" + initialContent.substring(1, initialContent.length() - 1) + ", " + value + "]");
        } else {
            appendString(colIndex, "[" + value + "]");
        }
    }

    void appendBoolean(final int colIndex, final boolean value) {
        if(rowSkipped) {
            booleanColumns[colIndex].set(currentRownum, value);            
        } else {
            booleanColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendShort(final int colIndex, final short value) {
        if(rowSkipped) {
            shortColumns[colIndex].set(currentRownum, value);            
        } else {
            shortColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendInt(final int colIndex, final int value) {
        if(rowSkipped) {
            intColumns[colIndex].set(currentRownum, value);            
        } else {
            intColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendLong(final int colIndex, final long value) {
        if(rowSkipped) {
            longColumns[colIndex].set(currentRownum, value);            
        } else {
            longColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void appendDate(final int colIndex, final LocalDate value) {
        if(rowSkipped) {
            dateColumns[colIndex].set(currentRownum, value);            
        } else {
            dateColumns[colIndex].append(value);
        }
        rowColumnsSet[colIndex] = true;
    }

    void startRow() {
        currentRownum++;
    }

    void endRow() {
        for (int i = 0; i < rowColumnsSet.length; i++) {
            if (!rowColumnsSet[i]) {
                if(rowSkipped) {
                    table.column(i).setMissing(currentRownum); 
                } else {
                    table.column(i).appendMissing();
                }
            } else {
                rowColumnsSet[i] = false;
            }
        }
        rowSkipped = false;
    }

    Row getCurrentRow() {
        if (this.currentRow == null) {
            this.currentRow = table.row(currentRownum);
        } else {
            currentRow.at(currentRownum);
        }
        return currentRow;
    }

    // Needs to be Boolean as null is returned for missing value
    Boolean getBoolean(final int colIndex, final int rowIndex) {
        return booleanColumns[colIndex].get(rowIndex);
    }

    short getShort(final int colIndex, final int rowIndex) {
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
    
    int getDateAsEpochDay(final int colIndex, final int rowIndex) {
        return (int) PackedLocalDate.toEpochDay(dateColumns[colIndex].getIntInternal(rowIndex));
    }

    int getTimeAsMilliOfDay(final int colIndex, final int rowIndex) {
      return PackedLocalTime.getMillisecondOfDay(timeColumns[colIndex].getIntInternal(rowIndex));
    }

    long getInstantAsEpochMilli(final int colIndex, final int rowIndex) {
        return instantColumns[colIndex].get(rowIndex).toEpochMilli();
    }

    long getDateTimeAsEpochMilli(final int colIndex, final int rowIndex) {
        return dateTimeColumns[colIndex].get(rowIndex).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    Column<?> column(final int colIndex) {
        return table.column(colIndex);
    }

    void skipCurrentRow() {
        rowSkipped = true;
        currentRownum--;
    }
    
    Table getTable() {
        if(rowSkipped) return table.dropRows(currentRownum + 1);
        return table;
    }
}
