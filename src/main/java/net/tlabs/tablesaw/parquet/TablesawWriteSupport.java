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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

public class TablesawWriteSupport extends WriteSupport<Row> {

    private enum FieldRecorder {
        BOOLEAN(ColumnType.BOOLEAN) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addBoolean(tableProxy.getBoolean(colIndex, rowNumber));
            }
        },
        SHORT(ColumnType.SHORT) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addInteger(tableProxy.getShort(colIndex, rowNumber));
            }
        },
        INTEGER(ColumnType.INTEGER){
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addInteger(tableProxy.getInt(colIndex, rowNumber));
            }
        },
        LONG(ColumnType.LONG) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addLong(tableProxy.getLong(colIndex, rowNumber));
            }
        },
        FLOAT(ColumnType.FLOAT){
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addFloat(tableProxy.getFloat(colIndex, rowNumber));
            }
        },
        DOUBLE(ColumnType.DOUBLE) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addDouble(tableProxy.getDouble(colIndex, rowNumber));
            }
        },
        STRING(ColumnType.STRING) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addBinary(Binary.fromString(tableProxy.getString(colIndex, rowNumber)));
            }
        },
        LOCAL_DATE(ColumnType.LOCAL_DATE) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addInteger(tableProxy.getDateAsEpochDay(colIndex, rowNumber));
            }
        },
        LOCAL_TIME(ColumnType.LOCAL_TIME) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addInteger(tableProxy.getTimeAsMilliOfDay(colIndex, rowNumber));
            }
        },
        LOCAL_DATE_TIME(ColumnType.LOCAL_DATE_TIME) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addLong(tableProxy.getDateTimeAsEpochMilli(colIndex, rowNumber));
            }
        },
        INSTANT(ColumnType.INSTANT) {
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                recordConsumer.addLong(tableProxy.getInstantAsEpochMilli(colIndex, rowNumber));
            }
        };

        final ColumnType columnType;
        
        private FieldRecorder(final ColumnType columnType) {
            this.columnType = columnType;
        }
        
        abstract void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                final int colIndex, final int rowNumber);
        
    }
    
    private static final String WRITE_SUPPORT_NAME = "net.tlabs.tablesaw.parquet";
    private static final Map<ColumnType, PrimitiveTypeName> PRIMITIVE_MAPPING;
    private static final Map<ColumnType, LogicalTypeAnnotation> ANNOTATION_MAPPING;
    private static final Map<ColumnType, FieldRecorder> RECORDER_MAPPING;
    private final TableProxy proxy;
    private final MessageType schema;
    private final int nbfields;
    private RecordConsumer recordConsumer;
    private FieldRecorder[] fieldRecorders;

    static {
        PRIMITIVE_MAPPING = new HashMap<>();
        PRIMITIVE_MAPPING.put(ColumnType.BOOLEAN, PrimitiveTypeName.BOOLEAN);
        PRIMITIVE_MAPPING.put(ColumnType.DOUBLE, PrimitiveTypeName.DOUBLE);
        PRIMITIVE_MAPPING.put(ColumnType.FLOAT, PrimitiveTypeName.FLOAT);
        PRIMITIVE_MAPPING.put(ColumnType.SHORT, PrimitiveTypeName.INT32);
        PRIMITIVE_MAPPING.put(ColumnType.INTEGER, PrimitiveTypeName.INT32);
        PRIMITIVE_MAPPING.put(ColumnType.LONG, PrimitiveTypeName.INT64);
        PRIMITIVE_MAPPING.put(ColumnType.INSTANT, PrimitiveTypeName.INT64);
        PRIMITIVE_MAPPING.put(ColumnType.LOCAL_DATE, PrimitiveTypeName.INT32);
        PRIMITIVE_MAPPING.put(ColumnType.LOCAL_TIME, PrimitiveTypeName.INT32);
        PRIMITIVE_MAPPING.put(ColumnType.LOCAL_DATE_TIME, PrimitiveTypeName.INT64);
        PRIMITIVE_MAPPING.put(ColumnType.STRING, PrimitiveTypeName.BINARY);
        ANNOTATION_MAPPING = new HashMap<>();
        ANNOTATION_MAPPING.put(ColumnType.SHORT, LogicalTypeAnnotation.intType(16, true));
        ANNOTATION_MAPPING.put(ColumnType.LOCAL_DATE, LogicalTypeAnnotation.dateType());
        ANNOTATION_MAPPING.put(ColumnType.LOCAL_TIME, LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS));
        ANNOTATION_MAPPING.put(ColumnType.INSTANT, LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS));
        ANNOTATION_MAPPING.put(ColumnType.LOCAL_DATE_TIME, LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS));
        ANNOTATION_MAPPING.put(ColumnType.STRING, LogicalTypeAnnotation.stringType());
        RECORDER_MAPPING = new HashMap<>();
        RECORDER_MAPPING.put(ColumnType.BOOLEAN, FieldRecorder.BOOLEAN);
        RECORDER_MAPPING.put(ColumnType.SHORT, FieldRecorder.SHORT);
        RECORDER_MAPPING.put(ColumnType.INTEGER, FieldRecorder.INTEGER);
        RECORDER_MAPPING.put(ColumnType.LONG, FieldRecorder.LONG);
        RECORDER_MAPPING.put(ColumnType.FLOAT, FieldRecorder.FLOAT);
        RECORDER_MAPPING.put(ColumnType.DOUBLE, FieldRecorder.DOUBLE);
        RECORDER_MAPPING.put(ColumnType.LOCAL_DATE, FieldRecorder.LOCAL_DATE);
        RECORDER_MAPPING.put(ColumnType.LOCAL_TIME, FieldRecorder.LOCAL_TIME);
        RECORDER_MAPPING.put(ColumnType.LOCAL_DATE_TIME, FieldRecorder.LOCAL_DATE_TIME);
        RECORDER_MAPPING.put(ColumnType.INSTANT, FieldRecorder.INSTANT);
        RECORDER_MAPPING.put(ColumnType.STRING, FieldRecorder.STRING);
    }

    public TablesawWriteSupport(final Table table) {
        super();
        this.proxy = new TableProxy(table);
        this.schema = internalCreateSchema(table);
        this.nbfields = schema.getFieldCount();
        this.fieldRecorders = internalCreateRecorders(table);
    }

    private static FieldRecorder[] internalCreateRecorders(final Table table) {
        return table.columns().stream()
            .map(Column::type)
            .map(RECORDER_MAPPING::get)
            .collect(Collectors.toList())
            .toArray(new FieldRecorder[0]);
    }
    
    public static MessageType createSchema(final Table table) {
        return internalCreateSchema(table);
    }

    private static MessageType internalCreateSchema(final Table table) {
        final String tableName = table.name();
        return new MessageType(tableName == null ? "message" : tableName,
            table.columns().stream()
            .map(TablesawWriteSupport::createType)
            .collect(Collectors.toList()));
    }

    private static Type createType(final Column<?> column) {
        final ColumnType type = column.type();
        return Types.optional(PRIMITIVE_MAPPING.get(type)).as(ANNOTATION_MAPPING.get(type)).named(column.name());
    }

    @Override
    public WriteContext init(final Configuration configuration) {
        return new WriteContext(this.schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(final RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(final Row row) {
        recordConsumer.startMessage();
        recordRow(row);
        recordConsumer.endMessage();
    }

    private void recordRow(final Row row) {
        final int rowNumber = row.getRowNumber();
        for (int colIndex = 0; colIndex < nbfields; colIndex++) {
            final Column<?> column = proxy.column(colIndex);
            if (!column.isMissing(rowNumber)) {
                final String fieldName = column.name();
                recordConsumer.startField(fieldName, colIndex);
                fieldRecorders[colIndex].recordValue(recordConsumer, proxy, colIndex, rowNumber);
                recordConsumer.endField(fieldName, colIndex);
            }
        }
    }

    @Override
    public String getName() {
        return WRITE_SUPPORT_NAME;
    }
}
