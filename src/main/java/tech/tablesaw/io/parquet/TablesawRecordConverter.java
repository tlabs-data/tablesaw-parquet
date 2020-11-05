package tech.tablesaw.io.parquet;

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
import java.util.Optional;

import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.read.SimpleRecordConverter;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

public class TablesawRecordConverter extends GroupConverter {

	private static final long SECOND_TO_MILLIS = 1_000l;

	private static final long SECOND_TO_MICROS = 1_000_000l;

	private static final long SECOND_TO_NANOS = 1_000_000_000l;

	private static final long MICRO_TO_NANO = 1_000l;

	private static final long MILLIS_TO_MICRO = 1_000l;

	private static final long MILLIS_TO_NANO = 1_000_000l;

	private static final Converter PRIMITIVE_SKIP_CONVERTER = new PrimitiveConverter() {
		@Override
		public void addBinary(Binary value) {
		}
		@Override
		public void addBoolean(boolean value) {
		}
		@Override
		public void addDouble(double value) {
		}
		@Override
		public void addFloat(float value) {
		}
		@Override
		public void addInt(int value) {
		}
		@Override
		public void addLong(long value) {
		}
		
	};
	private final Table table;
	private final Converter[] converters;
	private Row currentRow = null;
	
	public TablesawRecordConverter(final Table table, final MessageType fileSchema, final TablesawParquetReadOptions options) {
		super();
		this.table = table;
		this.converters = new Converter[fileSchema.getFieldCount()];
		for(final Column<?> column : table.columns()) {
			final String columnName = column.name();
			final int fieldIndex = fileSchema.getFieldIndex(columnName);
			final Type type = fileSchema.getType(fieldIndex);
			if(type.isPrimitive()) {
				converters[fieldIndex] = createConverter(columnName, column.type(), type, options);
			} else {
				converters[fieldIndex] = new SimpleRecordConverter(type.asGroupType()) {
					@Override
					public void end() {
						currentRow.setText(columnName, this.record.toString());
					}
				};
			}
		}
		for(int i = 0; i < converters.length; i++) {
			if(converters[i] == null) {
				final Type type = fileSchema.getType(i);
				if (type.isPrimitive()) {
					converters[i] = PRIMITIVE_SKIP_CONVERTER;
				} else {
					converters[i] = new SimpleRecordConverter(type.asGroupType()) {
						@Override
						public void end() {
							// do nothing
						}
					};
				}
			}
		}
	}

	private Converter createConverter(final String columnName, final ColumnType columnType,
			final Type schemaType, final TablesawParquetReadOptions options) {
		if(columnType == ColumnType.BOOLEAN) {
			return new PrimitiveConverter() {
				@Override
				public void addBoolean(final boolean value) {
					currentRow.setBoolean(columnName, value);
				}
			};
		}
		if(columnType == ColumnType.INTEGER) {
			return new PrimitiveConverter() {
				@Override
				public void addInt(final int value) {
					currentRow.setInt(columnName, value);
				}
			};
		}
		if(columnType == ColumnType.LONG) {
			return new PrimitiveConverter() {
				@Override
				public void addLong(final long value) {
					currentRow.setLong(columnName, value);
				}
			};
		}
		if(columnType == ColumnType.FLOAT) {
			return new PrimitiveConverter() {
				@Override
				public void addFloat(final float value) {
					currentRow.setFloat(columnName, value);
				}
			};
		}
		if(columnType == ColumnType.DOUBLE) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return new PrimitiveConverter() {
					@Override
					public void addDouble(final double value) {
						currentRow.setDouble(columnName, value);
					}
				};
			}
			return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
				@Override
				public Optional<Converter> visit(final DecimalLogicalTypeAnnotation decimalLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							final BigDecimal bigd = new BigDecimal(new BigInteger(value.getBytes()), decimalLogicalType.getScale());
							currentRow.setDouble(columnName, bigd.doubleValue());
						}
						@Override
						public void addDouble(final double value) {
							currentRow.setDouble(columnName, value);
						}
					});
				}
			}).orElse(new PrimitiveConverter() {
				@Override
				public void addDouble(final double value) {
					currentRow.setDouble(columnName, value);
				}
			});
		}
		if(columnType == ColumnType.STRING) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return options.unnanotatedBinaryAsString ?
					new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							currentRow.setString(columnName, value.toStringUsingUTF8());
						}
					}:
					new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							currentRow.setString(columnName, rawBytesToHexString(value.getBytes()));
						}
					};
			}
			return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
				@Override
				public Optional<Converter> visit(final StringLogicalTypeAnnotation stringLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							currentRow.setString(columnName, value.toStringUsingUTF8());
						}
					});
				}
				@Override
				public Optional<Converter> visit(final EnumLogicalTypeAnnotation enumLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							currentRow.setString(columnName, value.toStringUsingUTF8());
						}
					});
				}
				@Override
				public Optional<Converter> visit(final IntervalLogicalTypeAnnotation intervalLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
						    Preconditions.checkArgument(value.length() == 12, "Must be 12 bytes");
						    final ByteBuffer buf = value.toByteBuffer();
						    buf.order(ByteOrder.LITTLE_ENDIAN);
							currentRow.setString(columnName, Period.ofMonths(buf.getInt()).plusDays(buf.getInt()).toString()
									+ Duration.ofMillis(buf.getInt()).toString().substring(1));
						}
					});
				}
			}).orElse(new PrimitiveConverter() {
				@Override
				public void addBinary(final Binary value) {
					currentRow.setString(columnName, rawBytesToHexString(value.getBytes()));
				}
			});
		}
		if(columnType == ColumnType.TEXT) {
			return new PrimitiveConverter() {
				@Override
				public void addBinary(final Binary value) {
					currentRow.setString(columnName, value.toStringUsingUTF8());
				}
			};
		}
		if(columnType == ColumnType.INSTANT) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return new PrimitiveConverter() {
					@Override
					public void addLong(long value) {
						currentRow.setInstant(columnName, Instant.ofEpochMilli(value));
					}
					@Override
					public void addBinary(final Binary value) {
					    Preconditions.checkArgument(value.length() == 12, "Must be 12 bytes");
					    final ByteBuffer buf = value.toByteBuffer();
					    buf.order(ByteOrder.LITTLE_ENDIAN);
					    final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, buf.getInt());
						currentRow.setInstant(columnName, ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant()
								.plus(buf.getLong(), ChronoUnit.NANOS));
					}
				};
			}
			return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
				@Override
				public Optional<Converter> visit(final TimestampLogicalTypeAnnotation timestampLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addLong(long value) {
							switch(timestampLogicalType.getUnit()) {
							case MICROS:
								final long millisFromMicro = value / MILLIS_TO_MICRO;
								currentRow.setInstant(columnName,
										Instant.ofEpochMilli(millisFromMicro)
										.plus(value - millisFromMicro * MILLIS_TO_MICRO, ChronoUnit.MICROS));
								break;
							case MILLIS:
								currentRow.setInstant(columnName, Instant.ofEpochMilli(value));
								break;
							case NANOS:
								final long millisFromNanos = value / MILLIS_TO_NANO;
								currentRow.setInstant(columnName,
										Instant.ofEpochMilli(millisFromNanos).plusNanos(value - millisFromNanos * MILLIS_TO_NANO));
								break;
							default:
								throw new UnsupportedOperationException(
										"This should never happen: TimeUnit is neither MILLIS, MICROS or NANOS in Timestamp");
							}
						}
						@Override
						public void addBinary(final Binary value) {
						    Preconditions.checkArgument(value.length() == 12, "Must be 12 bytes");
						    final ByteBuffer buf = value.toByteBuffer();
						    buf.order(ByteOrder.LITTLE_ENDIAN);
						    final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, buf.getInt());
							currentRow.setInstant(columnName, ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant()
									.plus(buf.getLong(), ChronoUnit.NANOS));
						}
					});
				}
			}).orElse(new PrimitiveConverter() {
				@Override
				public void addLong(long value) {
					currentRow.setInstant(columnName, Instant.ofEpochMilli(value));
				}
				@Override
				public void addBinary(final Binary value) {
				    Preconditions.checkArgument(value.length() == 12, "Must be 12 bytes");
				    final ByteBuffer buf = value.toByteBuffer();
				    buf.order(ByteOrder.LITTLE_ENDIAN);
				    final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, buf.getInt());
					currentRow.setInstant(columnName, ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant()
							.plus(buf.getLong(), ChronoUnit.NANOS));
				}
			});
		}
		if(columnType == ColumnType.LOCAL_DATE) {
			return new PrimitiveConverter() {
				@Override
				public void addInt(int value) {
					currentRow.setDate(columnName, LocalDate.ofEpochDay(value));				}
			};
		}
		if(columnType == ColumnType.LOCAL_TIME) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return new PrimitiveConverter() {
					@Override
					public void addInt(int value) {
						currentRow.setTime(columnName, LocalTime.ofNanoOfDay(MILLIS_TO_NANO * value));
					}
					@Override
					public void addLong(long value) {
						currentRow.setTime(columnName, LocalTime.ofNanoOfDay(value));
					}
				};
			}
			return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
				@Override
				public Optional<Converter> visit(TimeLogicalTypeAnnotation timeLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addLong(long value) {
							switch(timeLogicalType.getUnit()) {
							case MICROS:
								currentRow.setTime(columnName, LocalTime.ofNanoOfDay(value * MICRO_TO_NANO));
								break;
							case NANOS:
								currentRow.setTime(columnName,LocalTime.ofNanoOfDay(value));
								break;
							default:
								throw new UnsupportedOperationException(
										"This should never happen: TimeUnit is neither MICROS or NANOS in Int64 Time");					
							}
						}
					});
				}
			}).orElse(new PrimitiveConverter() {
				@Override
				public void addInt(int value) {
					currentRow.setTime(columnName, LocalTime.ofNanoOfDay(MILLIS_TO_NANO * value));
				}
				@Override
				public void addLong(long value) {
					currentRow.setTime(columnName, LocalTime.ofNanoOfDay(value));
				}
			});
		}
		if(columnType == ColumnType.LOCAL_DATE_TIME) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return new PrimitiveConverter() {
					@Override
					public void addLong(long value) {
						final long epochSecond = value / SECOND_TO_MILLIS;
						currentRow.setDateTime(columnName, LocalDateTime.ofEpochSecond(epochSecond,
								(int)((value - (epochSecond * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
					}
				};
			}
			return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
				@Override
				public Optional<Converter> visit(final TimestampLogicalTypeAnnotation timestampLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addLong(long value) {
							switch(timestampLogicalType.getUnit()) {
							case MICROS:
								final long epochSecondFromMicros = value / SECOND_TO_MICROS;
								currentRow.setDateTime(columnName, LocalDateTime.ofEpochSecond(epochSecondFromMicros,
										(int)(value - (epochSecondFromMicros * SECOND_TO_MICROS) * MICRO_TO_NANO), ZoneOffset.UTC));
								break;
							case MILLIS:
								final long epochSecondFromMillis = value / SECOND_TO_MILLIS;
								currentRow.setDateTime(columnName, LocalDateTime.ofEpochSecond(epochSecondFromMillis,
										(int)((value - (epochSecondFromMillis * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
								break;
							case NANOS:
								final long epochSecondFromNanos = value / SECOND_TO_NANOS;
								currentRow.setDateTime(columnName, LocalDateTime.ofEpochSecond(epochSecondFromNanos,
										(int)((value - (epochSecondFromNanos * SECOND_TO_NANOS))), ZoneOffset.UTC));
								break;
							default:
								throw new UnsupportedOperationException(
										"This should never happen: TimeUnit is neither MILLIS, MICROS or NANOS in DateTime");
							}					
						}
					});
				}
			}).orElse(new PrimitiveConverter() {
				@Override
				public void addLong(long value) {
					final long epochSecond = value / SECOND_TO_MILLIS;
					currentRow.setDateTime(columnName, LocalDateTime.ofEpochSecond(epochSecond,
							(int)((value - (epochSecond * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
				}
			});
		}
		return null;
	}

	private static String rawBytesToHexString(final byte[] bytes) {
		final String[] hexBytes = new String[bytes.length];
		for(int i = 0; i < bytes.length; i++) {
			hexBytes[i] = String.format("%02X", bytes[i]);
		}
		return String.join(" ", hexBytes);
	}

	@Override
	public Converter getConverter(int fieldIndex) {
		return converters[fieldIndex];
	}

	@Override
	public void start() {
		if(this.currentRow == null || table.columnCount() == 0) {
			this.currentRow = this.table.appendRow();
		} else {
			for(Column<?> col : table.columns()) {
				col.appendMissing();
			}
			this.currentRow = this.currentRow.next();
		}
	}

	@Override
	public void end() {
		// nothing to do
	}

	public Row getCurrentRow() {
		return currentRow;
	}
	
	
}
