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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.JulianFields;
import java.util.List;
import java.util.Optional;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions.UnnanotatedBinaryAs;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.read.SimpleRecordConverter;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

public class TablesawRecordConverter extends GroupConverter {

    private static final int BINARY_INSTANT_LENGTH_VALUE = 12;
    private static final String BINARY_INSTANT_LENGTH_MESSAGE = "Must be 12 bytes";
    private static final int BINARY_INTERVAL_LENGTH_VALUE = 12;
    private static final String BINARY_INTERVAL_LENGTH_MESSAGE = "Must be 12 bytes";

    private final class DateTimePrimitiveConverter extends PrimitiveConverter {
        private final int colIndex;
        private final long secondFactor;
        private final long nanoFactor;

        private DateTimePrimitiveConverter(final int colIndex, final long secondFactor, final long nanoFactor) {
            super();
            this.colIndex = colIndex;
            this.secondFactor = secondFactor;
            this.nanoFactor = nanoFactor;
        }

        @Override
        public void addLong(final long value) {
            final long epochSecond = value / secondFactor;
            proxy.setDateTime(colIndex, LocalDateTime.ofEpochSecond(epochSecond,
                (int) ((value - (epochSecond * secondFactor)) * nanoFactor), ZoneOffset.UTC));
        }
        
    }
    
    private abstract class InstantPrimitiveConverter extends PrimitiveConverter {
        protected final int colIndex;

        private InstantPrimitiveConverter(final int colIndex) {
            super();
            this.colIndex = colIndex;
        }
        
        @Override
        public void addBinary(final Binary value) {
            Preconditions.checkArgument(value.length() == BINARY_INSTANT_LENGTH_VALUE, BINARY_INSTANT_LENGTH_MESSAGE);
            final ByteBuffer buf = value.toByteBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            final long nanotime = buf.getLong();
            final int julianday = buf.getInt();
            final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, julianday);
            proxy.setInstant(colIndex,
                ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant().plus(nanotime, ChronoUnit.NANOS));
        }
    }
    
    private final class MillisInstantPrimitiveConverter extends InstantPrimitiveConverter {
        private MillisInstantPrimitiveConverter(final int colIndex) {
            super(colIndex);
        }

        @Override
        public void addLong(final long value) {
            proxy.setInstant(colIndex, Instant.ofEpochMilli(value));
        }
    }
    
    private final class SubMillisInstantPrimitiveConverter extends InstantPrimitiveConverter {
        private final long factor;
        private final ChronoUnit chronoUnit;

        private SubMillisInstantPrimitiveConverter(final int colIndex, final long factor, final ChronoUnit unit) {
            super(colIndex);
            this.factor = factor;
            this.chronoUnit = unit;
        }

        @Override
        public void addLong(final long value) {
            final long millisFromValue = value / factor;
            proxy.setInstant(colIndex, Instant.ofEpochMilli(millisFromValue)
                .plus(value - millisFromValue * factor, chronoUnit));
        }
    }

    private final class DefaultDoublePrimitiveConverter extends PrimitiveConverter {
        private final int colIndex;

        private DefaultDoublePrimitiveConverter(final int colIndex) {
            this.colIndex = colIndex;
        }

        @Override
        public void addFloat(final float value) {
            proxy.setDouble(colIndex, value);
        }

        @Override
        public void addDouble(final double value) {
            proxy.setDouble(colIndex, value);
        }
    }

    private final class TimePrimitiveConverter  extends PrimitiveConverter {
        private final int colIndex;
        private final long longValueFactor;

        private TimePrimitiveConverter(final int colIndex, final long longValueFactor) {
            super();
            this.colIndex = colIndex;
            this.longValueFactor = longValueFactor;
        }
        
        @Override
        public void addInt(final int value) {
            // INT32 is always in MILLIS
            proxy.setTime(colIndex, LocalTime.ofNanoOfDay(MILLIS_TO_NANOS * value));
        }
        
        @Override
        public void addLong(final long value) {
            // INT64 is either MICROS or NANOS
            proxy.setTime(colIndex, LocalTime.ofNanoOfDay(value * longValueFactor));
        }
    }
    
    private final class StringPrimitiveConverter extends PrimitiveConverter {
        private final int colIndex;

        private StringPrimitiveConverter(final int colIndex) {
            this.colIndex = colIndex;
        }

        @Override
        public void addBinary(final Binary value) {
            proxy.setString(colIndex, value.toStringUsingUTF8());
        }
    }

    private final class HexStringPrimitiveConverter extends PrimitiveConverter {
        private final int colIndex;

        private HexStringPrimitiveConverter(final int colIndex) {
            this.colIndex = colIndex;
        }

        @Override
        public void addBinary(final Binary value) {
            proxy.setString(colIndex, rawBytesToHexString(value.getBytes()));
        }
    }

    private final class RepeatedAsTextConverter extends PrimitiveConverter {
        private final int col;

        private RepeatedAsTextConverter(final int col) {
            super();
            this.col = col;
        }
        
        private void addRepeatedValue(final String value) {
            proxy.setRepeatedString(col, value);
        }

        @Override
        public void addBinary(Binary value) {
            addRepeatedValue(value.toStringUsingUTF8());
        }

        @Override
        public void addBoolean(boolean value) {
            addRepeatedValue(Boolean.toString(value));
        }

        @Override
        public void addDouble(double value) {
            addRepeatedValue(Double.toString(value));
        }

        @Override
        public void addFloat(float value) {
            addRepeatedValue(Float.toString(value));
        }

        @Override
        public void addInt(int value) {
            addRepeatedValue(Integer.toString(value));
        }

        @Override
        public void addLong(long value) {
            addRepeatedValue(Long.toString(value));
        }

    }
    
    private final class GroupAsTextConverter extends SimpleRecordConverter {
        private final int col;

        private GroupAsTextConverter(final GroupType schema, final int col) {
            super(schema);
            this.col = col;
        }

        @Override
        public void end() {
            proxy.setString(col, this.record.toString());
        }
    }

    private static final long SECOND_TO_MILLIS = 1_000L;

    private static final long SECOND_TO_MICROS = 1_000_000L;

    private static final long SECOND_TO_NANOS = 1_000_000_000L;

    private static final long MICROS_TO_NANOS = 1_000L;

    private static final long MILLIS_TO_MICRO = 1_000L;

    private static final long MILLIS_TO_NANOS = 1_000_000L;

    private final Converter[] converters;
    private final SingleRowTableProxy proxy;

    public TablesawRecordConverter(final Table table, final MessageType fileSchema,
        final TablesawParquetReadOptions options) {
        super();
        this.proxy = new SingleRowTableProxy(table);
        this.converters = new Converter[fileSchema.getFieldCount()];
        final List<Column<?>> columns = table.columns();
        final int size = columns.size();
        for (int i = 0; i < size; i++) {
            final Column<?> column = columns.get(i);
            final ColumnType columnType = column.type();
            final int fieldIndex = fileSchema.getFieldIndex(column.name());
            final Type type = fileSchema.getType(fieldIndex);
            if (type.isPrimitive()) {
                if (type.isRepetition(Repetition.REPEATED)) {
                    converters[fieldIndex] = new RepeatedAsTextConverter(i);
                } else {
                    converters[fieldIndex] = createConverter(i, columnType, type, options);
                }
            } else {
                converters[fieldIndex] = new GroupAsTextConverter(type.asGroupType(), i);
            }
        }
    }

    private Converter createConverter(final int colIndex, final ColumnType columnType, final Type schemaType,
        final TablesawParquetReadOptions options) {
        if (ColumnType.INTEGER.equals(columnType)) {
            return new PrimitiveConverter() {
                @Override
                public void addInt(final int value) {
                    proxy.setInt(colIndex, value);
                }
            };
        }
        if (ColumnType.LONG.equals(columnType)) {
            return new PrimitiveConverter() {
                @Override
                public void addInt(final int value) {
                    proxy.setLong(colIndex, value);
                }
                @Override
                public void addLong(final long value) {
                    proxy.setLong(colIndex, value);
                }
            };
        }
        if (ColumnType.DOUBLE.equals(columnType)) {
            return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
                .flatMap(a -> doubleFromBinaryConverter(colIndex, a))
                .orElseGet(() -> new DefaultDoublePrimitiveConverter(colIndex));
        }
        if (ColumnType.BOOLEAN.equals(columnType)) {
            return new PrimitiveConverter() {
                @Override
                public void addBoolean(final boolean value) {
                    proxy.setBoolean(colIndex, value);
                }
            };
        }
        if (ColumnType.STRING.equals(columnType)) {
            return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
                .flatMap(a -> annotatedStringConverter(colIndex, a))
                .orElseGet(() -> createUnannotatedStringConverter(colIndex, schemaType, options));
        }
        if (ColumnType.FLOAT.equals(columnType)) {
            return new PrimitiveConverter() {
                @Override
                public void addFloat(final float value) {
                    proxy.setFloat(colIndex, value);
                }
            };
        }
        if (ColumnType.INSTANT.equals(columnType)) {
            return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
                .flatMap(a -> annotatedInstantConverter(colIndex, a))
                .orElseGet(() -> new MillisInstantPrimitiveConverter(colIndex));
        }
        if (ColumnType.LOCAL_DATE_TIME.equals(columnType)) {
            return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
                .flatMap(a -> annotatedDateTimeConverter(colIndex, a))
                .orElseGet(() -> new DateTimePrimitiveConverter(colIndex, SECOND_TO_MILLIS, MILLIS_TO_NANOS));
        }
        if (ColumnType.LOCAL_DATE.equals(columnType)) {
            return new PrimitiveConverter() {
                @Override
                public void addInt(final int value) {
                    proxy.setDate(colIndex, LocalDate.ofEpochDay(value));
                }
            };
        }
        if (ColumnType.LOCAL_TIME.equals(columnType)) {
            return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
                .flatMap(a -> annotatedTimeConverter(colIndex, a))
                .orElseGet(() -> new TimePrimitiveConverter(colIndex, 1L));
        }
        if (ColumnType.SHORT.equals(columnType)) {
            return new PrimitiveConverter() {
                @Override
                public void addInt(final int value) {
                    proxy.setShort(colIndex, (short) value);
                }
            };
        }
        return null;
    }

    private Optional<Converter> annotatedDateTimeConverter(final int colIndex, final LogicalTypeAnnotation annotation) {
        return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
            @Override
            public Optional<Converter> visit(final TimestampLogicalTypeAnnotation timestampLogicalType) {
                switch (timestampLogicalType.getUnit()) {
                    case MILLIS:
                        return Optional.of(new DateTimePrimitiveConverter(colIndex, SECOND_TO_MILLIS, MILLIS_TO_NANOS));
                    case MICROS:
                        return Optional.of(new DateTimePrimitiveConverter(colIndex, SECOND_TO_MICROS, MICROS_TO_NANOS));
                    case NANOS:
                        return Optional.of(new DateTimePrimitiveConverter(colIndex, SECOND_TO_NANOS, 1L));
                    default:
                        throw new UnsupportedOperationException(
                            "This should never happen: TimeUnit is neither MILLIS, MICROS or NANOS in DateTime");
                }
            }
        });
    }

    private Optional<Converter> annotatedTimeConverter(final int colIndex, final LogicalTypeAnnotation annotation) {
        return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
            @Override
            public Optional<Converter> visit(final TimeLogicalTypeAnnotation timeLogicalType) {
                switch (timeLogicalType.getUnit()) {
                    case MILLIS:
                        return Optional.of(new TimePrimitiveConverter(colIndex, MILLIS_TO_NANOS));
                    case MICROS:
                        return Optional.of(new TimePrimitiveConverter(colIndex, MICROS_TO_NANOS));
                    case NANOS:
                        return Optional.of(new TimePrimitiveConverter(colIndex, 1L));
                    default:
                        throw new UnsupportedOperationException(
                            "This should never happen: TimeUnit is neither MICROS or NANOS in Int64 Time");
                }
            }
        });
    }

    private Optional<Converter> annotatedInstantConverter(final int colIndex, final LogicalTypeAnnotation annotation) {
        return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
            @Override
            public Optional<Converter> visit(final TimestampLogicalTypeAnnotation timestampLogicalType) {
                switch (timestampLogicalType.getUnit()) {
                    case MILLIS:
                        return Optional.of(new MillisInstantPrimitiveConverter(colIndex));
                    case MICROS:
                        return Optional.of(
                            new SubMillisInstantPrimitiveConverter(colIndex, MILLIS_TO_MICRO, ChronoUnit.MICROS));
                    case NANOS:
                        return Optional.of(
                            new SubMillisInstantPrimitiveConverter(colIndex, MILLIS_TO_NANOS, ChronoUnit.NANOS));
                    default:
                        throw new UnsupportedOperationException(
                            "This should never happen: TimeUnit is neither MILLIS, MICROS or NANOS in Timestamp");
                }
            }
        });
    }

    private Optional<Converter> annotatedStringConverter(final int colIndex, final LogicalTypeAnnotation annotation) {
        return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
            @Override
            public Optional<Converter> visit(final StringLogicalTypeAnnotation stringLogicalType) {
                return Optional.of(new StringPrimitiveConverter(colIndex));
            }

            @Override
            public Optional<Converter> visit(final EnumLogicalTypeAnnotation enumLogicalType) {
                return Optional.of(new StringPrimitiveConverter(colIndex));
            }

            @Override
            public Optional<Converter> visit(final IntervalLogicalTypeAnnotation intervalLogicalType) {
                return Optional.of(new PrimitiveConverter() {
                    @Override
                    public void addBinary(final Binary value) {
                        Preconditions.checkArgument(value.length() == BINARY_INTERVAL_LENGTH_VALUE,
                            BINARY_INTERVAL_LENGTH_MESSAGE);
                        final ByteBuffer buf = value.toByteBuffer();
                        buf.order(ByteOrder.LITTLE_ENDIAN);
                        proxy.setString(colIndex,
                            Period.ofMonths(buf.getInt()).plusDays(buf.getInt()).toString()
                                + Duration.ofMillis(buf.getInt()).toString().substring(1));
                    }
                });
            }
        });
    }

    private Optional<Converter> doubleFromBinaryConverter(final int colIndex, final LogicalTypeAnnotation annotation) {
        return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
            @Override
            public Optional<Converter> visit(final DecimalLogicalTypeAnnotation decimalLogicalType) {
                return Optional.of(new PrimitiveConverter() {
                    @Override
                    public void addBinary(final Binary value) {
                        final BigDecimal bigd = new BigDecimal(new BigInteger(value.getBytes()), decimalLogicalType.getScale());
                        proxy.setDouble(colIndex, bigd.doubleValue());
                    }
                });
            }
        });
    }

    private Converter createUnannotatedStringConverter(final int colIndex, final Type schemaType,
            final TablesawParquetReadOptions options) {
        // INT96 as hex strings
        if (schemaType.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT96) {
            return new HexStringPrimitiveConverter(colIndex);
        }
        // Unannotated STRING depends on option
        // UnannotatedBinaryAs.SKIP case already treated
        return options.getUnnanotatedBinaryAs() == UnnanotatedBinaryAs.STRING ?
            new StringPrimitiveConverter(colIndex) : new HexStringPrimitiveConverter(colIndex);
    }

    private static String rawBytesToHexString(final byte[] bytes) {
        final String[] hexBytes = new String[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            hexBytes[i] = String.format("%02X", bytes[i]);
        }
        return String.join(" ", hexBytes);
    }

    @Override
    public Converter getConverter(final int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        proxy.startRow();
    }

    @Override
    public void end() {
        proxy.endRow();
    }

    public Row getCurrentRow() {
        return proxy.getCurrentRow();
    }
    
    public SingleRowTableProxy getTableProxy() {
        return proxy;
    }

}
