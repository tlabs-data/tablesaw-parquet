package net.tlabs.tablesaw.parquet;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import tech.tablesaw.api.Table;

final class SingleRowTableProxy extends TableProxy {
    
    SingleRowTableProxy(final Table table) {
        super(table.emptyCopy());
        this.table.appendRow();
        this.currentRownum = 0;
    }
    
    void setInstant(final int colIndex, final Instant value) {
        instantColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setDateTime(final int colIndex, final LocalDateTime value) {
        dateTimeColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setFloat(final int colIndex, final float value) {
        floatColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setDouble(final int colIndex, final double value) {
        doubleColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setTime(final int colIndex, final LocalTime value) {
        timeColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setString(final int colIndex, final String value) {
        stringColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setRepeatedString(final int colIndex, final String value) {
        if(rowColumnsSet[colIndex]) {
            final String initialContent = getString(colIndex, 0);
            stringColumns[colIndex].set(0, "[" + initialContent.substring(1, initialContent.length() - 1) + ", " + value + "]");
        } else {
            setString(colIndex, "[" + value + "]");
        }
    }

    void setBoolean(final int colIndex, final boolean value) {
        booleanColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setShort(final int colIndex, final short value) {
        shortColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setInt(final int colIndex, final int value) {
        intColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setLong(final int colIndex, final long value) {
        longColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    void setDate(final int colIndex, final LocalDate value) {
        dateColumns[colIndex].set(0, value);
        rowColumnsSet[colIndex] = true;
    }

    @Override
    void startRow() {
        // do nothing
    }

    @Override
    void endRow() {
        for (int i = 0; i < rowColumnsSet.length; i++) {
            if (!rowColumnsSet[i]) {
                table.column(i).setMissing(0);
            } else {
                rowColumnsSet[i] = false;
            }
        }
    }

}
