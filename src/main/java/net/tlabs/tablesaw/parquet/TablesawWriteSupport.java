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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Period;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

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
        UUID(ColumnType.STRING) {
            private final ByteBuffer buffer = ByteBuffer.allocateDirect(16);
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                    final int colIndex, final int rowNumber) {
                final UUID uuid = java.util.UUID.fromString(tableProxy.getString(colIndex, rowNumber));
                buffer
                    .clear()
                    .putLong(uuid.getMostSignificantBits())
                    .putLong(uuid.getLeastSignificantBits())
                    .rewind();
                recordConsumer.addBinary(Binary.fromReusedByteBuffer(buffer));
            }
        },
        BSON(ColumnType.STRING) {
            private final DocumentCodec codec = new DocumentCodec();
            private final BasicOutputBuffer buffer = new BasicOutputBuffer();
            private final EncoderContext encoderContext = EncoderContext.builder().build();
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy, 
                    final int colIndex, final int rowNumber) {
                final Document doc = Document.parse(tableProxy.getString(colIndex, rowNumber));
                buffer.truncateToPosition(0);
                codec.encode(new BsonBinaryWriter(buffer), doc, encoderContext);
                recordConsumer.addBinary(Binary.fromReusedByteArray(buffer.getInternalBuffer()));
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
        },
        INTERVAL(ColumnType.STRING) {
            private final ByteBuffer buffer = ByteBuffer.allocateDirect(12).order(ByteOrder.LITTLE_ENDIAN);
            private final Period emptyPeriod = Period.of(0, 0, 0);
            private final Duration emptyDuration = Duration.ofMillis(0);
            @Override
            void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy, 
                    final int colIndex, final int rowNumber) {
                final String[] values = tableProxy.getString(colIndex, rowNumber).split(INTERVAL_TIME_DESIGNATOR);
                // Handle no period only duration (PTxxx) case, as "P" is not a valid Period
                final Period period = values[0].length() > 1 ? Period.parse(values[0]) : emptyPeriod;
                // Handle no duration only period (Pxxx) case, as T is omitted
                final Duration duration = values.length > 1 ?
                    Duration.parse(new StringBuilder(INTERVAL_TIME_ONLY_DESIGNATOR).append(values[1]).toString()) : emptyDuration;
                buffer
                    .clear()
                    .putInt(period.getMonths())
                    .putInt(period.getDays())
                    .putInt((int)duration.toMillis())
                    .rewind();
                recordConsumer.addBinary(Binary.fromReusedByteBuffer(buffer));
            }
        };

        private static final String INTERVAL_TIME_ONLY_DESIGNATOR = "PT";
        private static final String INTERVAL_TIME_DESIGNATOR = "T";
        final ColumnType columnType;
        
        private FieldRecorder(final ColumnType columnType) {
            this.columnType = columnType;
        }
        
        abstract void recordValue(final RecordConsumer recordConsumer, final TableProxy tableProxy,
                final int colIndex, final int rowNumber);

        void validate(final ColumnType type) {
            if(!this.columnType.equals(type)) {
                throw new IllegalArgumentException(this.name() + " recorder needs a " 
                        + columnType.name() + " column, not a " + type.name() + " column");
            }
        }
        
    }
    
    private static final String WRITE_SUPPORT_NAME = "net.tlabs.tablesaw.parquet";
    private static final Map<ColumnType, PrimitiveTypeName> PRIMITIVE_MAPPING;
    private static final Map<ColumnType, LogicalTypeAnnotation> ANNOTATION_MAPPING;
    private static final Map<ColumnType, FieldRecorder> RECORDER_MAPPING;
    private static final Map<LogicalTypeAnnotation, FieldRecorder> LOGICALTYPE_RECORDER_MAPPING;
    private static final Map<LogicalTypeAnnotation, PrimitiveTypeName> LOGICALTYPE_MAPPING;
    private static final Map<LogicalTypeAnnotation, Integer> LOGICALTYPE_FIELD_LENGTH;
    private final TableProxy proxy;
    private final MessageType schema;
    private final int nbfields;
    private RecordConsumer recordConsumer;
    private final FieldRecorder[] fieldRecorders;
    private final Map<String, LogicalTypeAnnotation> typeMap;

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
        LOGICALTYPE_MAPPING = new HashMap<>();
        LOGICALTYPE_MAPPING.put(LogicalTypeAnnotation.uuidType(), PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
        LOGICALTYPE_MAPPING.put(LogicalTypeAnnotation.jsonType(), PrimitiveTypeName.BINARY);
        LOGICALTYPE_MAPPING.put(LogicalTypeAnnotation.bsonType(), PrimitiveTypeName.BINARY);
        LOGICALTYPE_MAPPING.put(LogicalTypeAnnotation.intervalType(), PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
        LOGICALTYPE_FIELD_LENGTH = new HashMap<>();
        LOGICALTYPE_FIELD_LENGTH.put(LogicalTypeAnnotation.uuidType(), 16);
        LOGICALTYPE_FIELD_LENGTH.put(LogicalTypeAnnotation.intervalType(), 12);
        LOGICALTYPE_RECORDER_MAPPING = new HashMap<>();
        LOGICALTYPE_RECORDER_MAPPING.put(LogicalTypeAnnotation.uuidType(), FieldRecorder.UUID);
        LOGICALTYPE_RECORDER_MAPPING.put(LogicalTypeAnnotation.jsonType(), FieldRecorder.STRING);
        LOGICALTYPE_RECORDER_MAPPING.put(LogicalTypeAnnotation.bsonType(), FieldRecorder.BSON);
        LOGICALTYPE_RECORDER_MAPPING.put(LogicalTypeAnnotation.intervalType(), FieldRecorder.INTERVAL);
    }

    public TablesawWriteSupport(final Table table) {
        this(table, Collections.emptyMap());
    }

    public TablesawWriteSupport(final Table table, final Map<String, LogicalTypeAnnotation> typeMap) {
        super();
        this.proxy = new TableProxy(table);
        this.typeMap = typeMap;
        this.schema = internalCreateSchema(table);
        this.nbfields = schema.getFieldCount();
        this.fieldRecorders = internalCreateRecorders(table);
    }

    private MessageType internalCreateSchema(final Table table) {
        final String tableName = table.name();
        return new MessageType(tableName == null ? "message" : tableName,
            table.columns().stream()
            .map(this::createType)
            .collect(Collectors.toList()));
    }

    private Type createType(final Column<?> column) {
        final ColumnType columnType = column.type();
        final String name = column.name();
        final PrimitiveTypeName primitiveType = LOGICALTYPE_MAPPING.getOrDefault(
            typeMap.get(name), PRIMITIVE_MAPPING.get(columnType));
        final PrimitiveBuilder<PrimitiveType> parquetType = Types
            .optional(primitiveType);
        final LogicalTypeAnnotation logicalType = typeMap.getOrDefault(name, ANNOTATION_MAPPING.get(columnType));
        // All FIXED_LEN_BYTE_ARRAY columns must have a logical type and a length entry
        if(primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            parquetType.length(LOGICALTYPE_FIELD_LENGTH.get(logicalType));
        }
        if(logicalType != null) {
            parquetType.as(logicalType);
        }
        return parquetType.named(name);
    }

    private FieldRecorder[] internalCreateRecorders(final Table table) {
        return table.columns().stream()
            .map(this::createRecorder)
            .collect(Collectors.toList())
            .toArray(new FieldRecorder[0]);
    }
    
    private FieldRecorder createRecorder(final Column<?> column) {
        final FieldRecorder recorder = LOGICALTYPE_RECORDER_MAPPING
            .getOrDefault(typeMap.get(column.name()), RECORDER_MAPPING.get(column.type()));
        recorder.validate(column.type());
        return recorder;
    }
    
    @SuppressWarnings("deprecation")
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
