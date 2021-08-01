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
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
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

	private final class DefaultDateTimePrimitiveConverter extends PrimitiveConverter {
		private final int colIndex;

		private DefaultDateTimePrimitiveConverter(int colIndex) {
			this.colIndex = colIndex;
		}

		@Override
		public void addLong(long value) {
			final long epochSecond = value / SECOND_TO_MILLIS;
			proxy.appendDateTime(colIndex, LocalDateTime.ofEpochSecond(epochSecond,
					(int) ((value - (epochSecond * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
		}
	}

	private final class DefaultInstantPrimitiveConverter extends PrimitiveConverter {
		private final int colIndex;

		private DefaultInstantPrimitiveConverter(int colIndex) {
			this.colIndex = colIndex;
		}

		@Override
		public void addLong(long value) {
			proxy.appendInstant(colIndex, Instant.ofEpochMilli(value));
		}

		@Override
		public void addBinary(final Binary value) {
			Preconditions.checkArgument(value.length() == BINARY_INSTANT_LENGTH_VALUE, BINARY_INSTANT_LENGTH_MESSAGE);
			final ByteBuffer buf = value.toByteBuffer();
			buf.order(ByteOrder.LITTLE_ENDIAN);
			final long nanotime = buf.getLong();
			final int julianday = buf.getInt();
			final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, julianday);
			proxy.appendInstant(colIndex,
					ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant().plus(nanotime, ChronoUnit.NANOS));
		}
	}

	private final class DefaultDoublePrimitiveConverter extends PrimitiveConverter {
		private final int colIndex;

		private DefaultDoublePrimitiveConverter(int colIndex) {
			this.colIndex = colIndex;
		}

		@Override
		public void addFloat(final float value) {
			proxy.appendDouble(colIndex, value);
		}

		@Override
		public void addDouble(final double value) {
			proxy.appendDouble(colIndex, value);
		}
	}

	private final class DefaultTimePrimitiveConverter extends PrimitiveConverter {
		private final int colIndex;

		private DefaultTimePrimitiveConverter(final int colIndex) {
			this.colIndex = colIndex;
		}

		@Override
		public void addInt(final int value) {
			proxy.appendTime(colIndex, LocalTime.ofNanoOfDay(MILLIS_TO_NANO * value));
		}

		@Override
		public void addLong(final long value) {
			proxy.appendTime(colIndex, LocalTime.ofNanoOfDay(value));
		}
	}

	private final class StringPrimitiveConverter extends PrimitiveConverter {
		private final int colIndex;

		private StringPrimitiveConverter(final int colIndex) {
			this.colIndex = colIndex;
		}

		@Override
		public void addBinary(final Binary value) {
			proxy.appendString(colIndex, value.toStringUsingUTF8());
		}
	}

	private final class HexStringPrimitiveConverter extends PrimitiveConverter {
		private final int colIndex;

		private HexStringPrimitiveConverter(final int colIndex) {
			this.colIndex = colIndex;
		}

		@Override
		public void addBinary(final Binary value) {
			proxy.appendString(colIndex, rawBytesToHexString(value.getBytes()));
		}
	}

	private final class GroupSkipConverter extends SimpleRecordConverter {
		private GroupSkipConverter(final GroupType schema) {
			super(schema);
		}

		@Override
		public void end() {
			// do nothing
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
			proxy.appendText(col, this.record.toString());
		}
	}

	private static final long SECOND_TO_MILLIS = 1_000l;

	private static final long SECOND_TO_MICROS = 1_000_000l;

	private static final long SECOND_TO_NANOS = 1_000_000_000l;

	private static final long MICRO_TO_NANO = 1_000l;

	private static final long MILLIS_TO_MICRO = 1_000l;

	private static final long MILLIS_TO_NANO = 1_000_000l;

	private static final Converter PRIMITIVE_SKIP_CONVERTER = new PrimitiveConverter() {
		@Override
		public void addBinary(Binary value) {
			/* SKIPPED */
		}

		@Override
		public void addBoolean(boolean value) {
			/* SKIPPED */
		}

		@Override
		public void addDouble(double value) {
			/* SKIPPED */
		}

		@Override
		public void addFloat(float value) {
			/* SKIPPED */
		}

		@Override
		public void addInt(int value) {
			/* SKIPPED */
		}

		@Override
		public void addLong(long value) {
			/* SKIPPED */
		}
	};
	private final Converter[] converters;
	private final TableProxy proxy;

	public TablesawRecordConverter(final Table table, final MessageType fileSchema,
			final TablesawParquetReadOptions options) {
		super();
		this.proxy = new TableProxy(table);
		this.converters = new Converter[fileSchema.getFieldCount()];
		final List<Column<?>> columns = table.columns();
		final int size = columns.size();
		for (int i = 0; i < size; i++) {
			final Column<?> column = columns.get(i);
			final ColumnType columnType = column.type();
			final int fieldIndex = fileSchema.getFieldIndex(column.name());
			final Type type = fileSchema.getType(fieldIndex);
			if (type.isPrimitive()) {
				converters[fieldIndex] = createConverter(i, columnType, type, options);
			} else {
				final int col = i;
				converters[fieldIndex] = new GroupAsTextConverter(type.asGroupType(), col);
			}
		}
		for (int i = 0; i < converters.length; i++) {
			if (converters[i] == null) {
				final Type type = fileSchema.getType(i);
				if (type.isPrimitive()) {
					converters[i] = PRIMITIVE_SKIP_CONVERTER;
				} else {
					converters[i] = new GroupSkipConverter(type.asGroupType());
				}
			}
		}
	}

	private Converter createConverter(final int colIndex, final ColumnType columnType, final Type schemaType,
			final TablesawParquetReadOptions options) {
		if (columnType == ColumnType.BOOLEAN) {
			return new PrimitiveConverter() {
				@Override
				public void addBoolean(final boolean value) {
					proxy.appendBoolean(colIndex, value);
				}
			};
		}
		if (columnType == ColumnType.SHORT) {
			return new PrimitiveConverter() {
				@Override
				public void addInt(final int value) {
					proxy.appendShort(colIndex, (short) value);
				}
			};
		}
		if (columnType == ColumnType.INTEGER) {
			return new PrimitiveConverter() {
				@Override
				public void addInt(final int value) {
					proxy.appendInt(colIndex, value);
				}
			};
		}
		if (columnType == ColumnType.LONG) {
			return new PrimitiveConverter() {
				@Override
				public void addLong(final long value) {
					proxy.appendLong(colIndex, value);
				}
			};
		}
		if (columnType == ColumnType.FLOAT) {
			return new PrimitiveConverter() {
				@Override
				public void addFloat(final float value) {
					proxy.appendFloat(colIndex, value);
				}
			};
		}
		if (columnType == ColumnType.DOUBLE) {
			return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
					.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Converter>() {
						@Override
						public Optional<Converter> visit(final DecimalLogicalTypeAnnotation decimalLogicalType) {
							return Optional.of(new PrimitiveConverter() {
								@Override
								public void addBinary(final Binary value) {
									final BigDecimal bigd = new BigDecimal(new BigInteger(value.getBytes()),
											decimalLogicalType.getScale());
									proxy.appendDouble(colIndex, bigd.doubleValue());
								}
							});
						}
					})).orElseGet(() -> new DefaultDoublePrimitiveConverter(colIndex));
		}
		if (columnType == ColumnType.STRING) {
			return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
					.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Converter>() {
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
									proxy.appendString(colIndex,
											Period.ofMonths(buf.getInt()).plusDays(buf.getInt()).toString()
													+ Duration.ofMillis(buf.getInt()).toString().substring(1));
								}
							});
						}
					}))
					.orElseGet(() -> schemaType.asPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.INT96
							&& options.getUnnanotatedBinaryAs() == UnnanotatedBinaryAs.STRING
									? new StringPrimitiveConverter(colIndex)
									: new HexStringPrimitiveConverter(colIndex));
		}
		if (columnType == ColumnType.TEXT) {
			return new PrimitiveConverter() {
				@Override
				public void addBinary(final Binary value) {
					proxy.appendText(colIndex, value.toStringUsingUTF8());
				}
			};
		}
		if (columnType == ColumnType.INSTANT) {
			return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
					.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Converter>() {
						@Override
						public Optional<Converter> visit(final TimestampLogicalTypeAnnotation timestampLogicalType) {
							return Optional.of(new PrimitiveConverter() {
								@Override
								public void addLong(long value) {
									switch (timestampLogicalType.getUnit()) {
									case MICROS:
										final long millisFromMicro = value / MILLIS_TO_MICRO;
										proxy.appendInstant(colIndex, Instant.ofEpochMilli(millisFromMicro)
												.plus(value - millisFromMicro * MILLIS_TO_MICRO, ChronoUnit.MICROS));
										break;
									case MILLIS:
										proxy.appendInstant(colIndex, Instant.ofEpochMilli(value));
										break;
									case NANOS:
										final long millisFromNanos = value / MILLIS_TO_NANO;
										proxy.appendInstant(colIndex, Instant.ofEpochMilli(millisFromNanos)
												.plusNanos(value - millisFromNanos * MILLIS_TO_NANO));
										break;
									default:
										throw new UnsupportedOperationException(
												"This should never happen: TimeUnit is neither MILLIS, MICROS or NANOS in Timestamp");
									}
								}

								@Override
								public void addBinary(final Binary value) {
									Preconditions.checkArgument(value.length() == BINARY_INSTANT_LENGTH_VALUE,
											BINARY_INSTANT_LENGTH_MESSAGE);
									final ByteBuffer buf = value.toByteBuffer();
									buf.order(ByteOrder.LITTLE_ENDIAN);
									final long nanoday = buf.getLong();
									final int julianday = buf.getInt();
									final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY,
											julianday);
									proxy.appendInstant(colIndex, ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC)
											.toInstant().plus(nanoday, ChronoUnit.NANOS));
								}
							});
						}
					})).orElseGet(() -> new DefaultInstantPrimitiveConverter(colIndex));
		}
		if (columnType == ColumnType.LOCAL_DATE) {
			return new PrimitiveConverter() {
				@Override
				public void addInt(int value) {
					proxy.appendDate(colIndex, LocalDate.ofEpochDay(value));
				}
			};
		}
		if (columnType == ColumnType.LOCAL_TIME) {
			return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
					.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Converter>() {
						@Override
						public Optional<Converter> visit(final TimeLogicalTypeAnnotation timeLogicalType) {
							return Optional.of(new PrimitiveConverter() {
								@Override
								public void addLong(long value) {
									switch (timeLogicalType.getUnit()) {
									case MICROS:
										proxy.appendTime(colIndex, LocalTime.ofNanoOfDay(value * MICRO_TO_NANO));
										break;
									case NANOS:
										proxy.appendTime(colIndex, LocalTime.ofNanoOfDay(value));
										break;
									default:
										throw new UnsupportedOperationException(
												"This should never happen: TimeUnit is neither MICROS or NANOS in Int64 Time");
									}
								}
							});
						}
					})).orElseGet(() -> new DefaultTimePrimitiveConverter(colIndex));
		}
		if (columnType == ColumnType.LOCAL_DATE_TIME) {
			return Optional.ofNullable(schemaType.getLogicalTypeAnnotation())
					.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Converter>() {
						@Override
						public Optional<Converter> visit(final TimestampLogicalTypeAnnotation timestampLogicalType) {
							return Optional.of(new PrimitiveConverter() {
								@Override
								public void addLong(long value) {
									switch (timestampLogicalType.getUnit()) {
									case MICROS:
										final long epochSecondFromMicros = value / SECOND_TO_MICROS;
										proxy.appendDateTime(colIndex,
												LocalDateTime.ofEpochSecond(epochSecondFromMicros, (int) (value
														- (epochSecondFromMicros * SECOND_TO_MICROS) * MICRO_TO_NANO),
														ZoneOffset.UTC));
										break;
									case MILLIS:
										final long epochSecondFromMillis = value / SECOND_TO_MILLIS;
										proxy.appendDateTime(colIndex,
												LocalDateTime.ofEpochSecond(epochSecondFromMillis,
														(int) ((value - (epochSecondFromMillis * SECOND_TO_MILLIS))
																* MILLIS_TO_NANO),
														ZoneOffset.UTC));
										break;
									case NANOS:
										final long epochSecondFromNanos = value / SECOND_TO_NANOS;
										proxy.appendDateTime(colIndex,
												LocalDateTime.ofEpochSecond(epochSecondFromNanos,
														(int) (value - (epochSecondFromNanos * SECOND_TO_NANOS)),
														ZoneOffset.UTC));
										break;
									default:
										throw new UnsupportedOperationException(
												"This should never happen: TimeUnit is neither MILLIS, MICROS or NANOS in DateTime");
									}
								}
							});
						}
					})).orElseGet(() -> new DefaultDateTimePrimitiveConverter(colIndex));
		}
		return null;
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
}
