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
import java.util.List;
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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.read.SimpleRecordConverter;

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
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.api.TextColumn;
import tech.tablesaw.api.TimeColumn;
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
	private int currentRownum = -1;
	private final boolean[] rowColumnsSet;
	private final BooleanColumn[] booleanColumns;
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
	
	public TablesawRecordConverter(final Table table, final MessageType fileSchema, final TablesawParquetReadOptions options) {
		super();
		this.table = table;
		this.converters = new Converter[fileSchema.getFieldCount()];
		final List<Column<?>> columns = table.columns();
		final int size = columns.size();
		rowColumnsSet = new boolean[size];
		booleanColumns = new BooleanColumn[size];
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
		for(int i = 0; i < size; i++) {
			final Column<?> column = columns.get(i);
			final ColumnType columnType = column.type();
			fillColumnArrays(i, columnType);
			final int fieldIndex = fileSchema.getFieldIndex(column.name());
			final Type type = fileSchema.getType(fieldIndex);
			if(type.isPrimitive()) {
				converters[fieldIndex] = createConverter(i, columnType, type, options);
			} else {
				final int col = i;
				converters[fieldIndex] = new SimpleRecordConverter(type.asGroupType()) {
					@Override
					public void end() {
						textColumns[col].append(this.record.toString());
						rowColumnsSet[col] = true;
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

	private void fillColumnArrays(final int i, final ColumnType columnType) {
		if(columnType == ColumnType.BOOLEAN) {
			booleanColumns[i] = table.booleanColumn(i);
		} else if (columnType == ColumnType.INTEGER) {
			intColumns[i] = table.intColumn(i);
		} else if (columnType == ColumnType.LONG) {
			longColumns[i] =table.longColumn(i);
		} else if(columnType == ColumnType.FLOAT) {
			floatColumns[i] = table.floatColumn(i);
		} else if(columnType == ColumnType.DOUBLE) {
			doubleColumns[i] = table.doubleColumn(i);
		} else if(columnType == ColumnType.LOCAL_TIME) {
			timeColumns[i] = table.timeColumn(i);
		} else if(columnType == ColumnType.LOCAL_DATE) {
			dateColumns[i] = table.dateColumn(i);
		} else if(columnType == ColumnType.LOCAL_DATE_TIME) {
			dateTimeColumns[i] = table.dateTimeColumn(i);
		} else if(columnType == ColumnType.INSTANT) {
			instantColumns[i] = table.instantColumn(i);
		} else if(columnType == ColumnType.STRING) {
			stringColumns[i] = table.stringColumn(i);
		} else if(columnType == ColumnType.TEXT) {
			textColumns[i] = table.textColumn(i);
		}
	}

	private Converter createConverter(final int colIndex, final ColumnType columnType,
			final Type schemaType, final TablesawParquetReadOptions options) {
		if(columnType == ColumnType.BOOLEAN) {
			return new PrimitiveConverter() {
				@Override
				public void addBoolean(final boolean value) {
					booleanColumns[colIndex].append(value);
					rowColumnsSet[colIndex] = true;
				}
			};
		}
		if(columnType == ColumnType.INTEGER) {
			return new PrimitiveConverter() {
				@Override
				public void addInt(final int value) {
					intColumns[colIndex].append(value);
					rowColumnsSet[colIndex] = true;
				}
			};
		}
		if(columnType == ColumnType.LONG) {
			return new PrimitiveConverter() {
				@Override
				public void addLong(final long value) {
					longColumns[colIndex].append(value);
					rowColumnsSet[colIndex] = true;
				}
			};
		}
		if(columnType == ColumnType.FLOAT) {
			return new PrimitiveConverter() {
				@Override
				public void addFloat(final float value) {
					floatColumns[colIndex].append(value);
					rowColumnsSet[colIndex] = true;
				}
			};
		}
		if(columnType == ColumnType.DOUBLE) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return new PrimitiveConverter() {
					@Override
					public void addDouble(final double value) {
						doubleColumns[colIndex].append(value);
						rowColumnsSet[colIndex] = true;
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
							doubleColumns[colIndex].append(bigd.doubleValue());
							rowColumnsSet[colIndex] = true;
						}
						@Override
						public void addDouble(final double value) {
							doubleColumns[colIndex].append(value);
							rowColumnsSet[colIndex] = true;
						}
					});
				}
			}).orElse(new PrimitiveConverter() {
				@Override
				public void addDouble(final double value) {
					doubleColumns[colIndex].append(value);
					rowColumnsSet[colIndex] = true;
				}
			});
		}
		if(columnType == ColumnType.STRING) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return schemaType.asPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.INT96 &&
						options.unnanotatedBinaryAsString ?
					new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							stringColumns[colIndex].append(value.toStringUsingUTF8());
							rowColumnsSet[colIndex] = true;
						}
					}:
					new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							stringColumns[colIndex].append(rawBytesToHexString(value.getBytes()));
							rowColumnsSet[colIndex] = true;
						}
					};
			}
			return annotation.accept(new LogicalTypeAnnotationVisitor<Converter>() {
				@Override
				public Optional<Converter> visit(final StringLogicalTypeAnnotation stringLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							stringColumns[colIndex].append(value.toStringUsingUTF8());
							rowColumnsSet[colIndex] = true;
						}
					});
				}
				@Override
				public Optional<Converter> visit(final EnumLogicalTypeAnnotation enumLogicalType) {
					return Optional.of(new PrimitiveConverter() {
						@Override
						public void addBinary(final Binary value) {
							stringColumns[colIndex].append(value.toStringUsingUTF8());
							rowColumnsSet[colIndex] = true;
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
						    stringColumns[colIndex].append(Period.ofMonths(buf.getInt()).plusDays(buf.getInt()).toString()
									+ Duration.ofMillis(buf.getInt()).toString().substring(1));
							rowColumnsSet[colIndex] = true;
						}
					});
				}
			}).orElse(new PrimitiveConverter() {
				@Override
				public void addBinary(final Binary value) {
					stringColumns[colIndex].append(rawBytesToHexString(value.getBytes()));
					rowColumnsSet[colIndex] = true;
				}
			});
		}
		if(columnType == ColumnType.TEXT) {
			return new PrimitiveConverter() {
				@Override
				public void addBinary(final Binary value) {
					textColumns[colIndex].append(value.toStringUsingUTF8());
					rowColumnsSet[colIndex] = true;
				}
			};
		}
		if(columnType == ColumnType.INSTANT) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return new PrimitiveConverter() {
					@Override
					public void addLong(long value) {
						instantColumns[colIndex].append( Instant.ofEpochMilli(value));
						rowColumnsSet[colIndex] = true;
					}
					@Override
					public void addBinary(final Binary value) {
					    Preconditions.checkArgument(value.length() == 12, "Must be 12 bytes");
					    final ByteBuffer buf = value.toByteBuffer();
					    buf.order(ByteOrder.LITTLE_ENDIAN);
						final long nanotime = buf.getLong();
					    final int juliaday = buf.getInt();
						final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, juliaday);
						instantColumns[colIndex].append(
								ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant()
								.plus(nanotime, ChronoUnit.NANOS));
						rowColumnsSet[colIndex] = true;
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
								instantColumns[colIndex].append( Instant.ofEpochMilli(millisFromMicro)
										.plus(value - millisFromMicro * MILLIS_TO_MICRO, ChronoUnit.MICROS));
								rowColumnsSet[colIndex] = true;
								break;
							case MILLIS:
								instantColumns[colIndex].append( Instant.ofEpochMilli(value));
								rowColumnsSet[colIndex] = true;
								break;
							case NANOS:
								final long millisFromNanos = value / MILLIS_TO_NANO;
								instantColumns[colIndex].set(currentRownum,
										Instant.ofEpochMilli(millisFromNanos).plusNanos(value - millisFromNanos * MILLIS_TO_NANO));
								rowColumnsSet[colIndex] = true;
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
							final long nanoday = buf.getLong();
						    final int julianday = buf.getInt();
							final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, julianday);
							instantColumns[colIndex].append(
									ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant()
									.plus(nanoday, ChronoUnit.NANOS));
							rowColumnsSet[colIndex] = true;
						}
					});
				}
			}).orElse(new PrimitiveConverter() {
				@Override
				public void addLong(long value) {
					instantColumns[colIndex].append( Instant.ofEpochMilli(value));
					rowColumnsSet[colIndex] = true;
				}
				@Override
				public void addBinary(final Binary value) {
				    Preconditions.checkArgument(value.length() == 12, "Must be 12 bytes");
				    final ByteBuffer buf = value.toByteBuffer();
				    buf.order(ByteOrder.LITTLE_ENDIAN);
					final long nanotime = buf.getLong();
				    final int juliaday = buf.getInt();
					final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, juliaday);
					instantColumns[colIndex].append(
							ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant()
							.plus(nanotime, ChronoUnit.NANOS));
					rowColumnsSet[colIndex] = true;
				}
			});
		}
		if(columnType == ColumnType.LOCAL_DATE) {
			return new PrimitiveConverter() {
				@Override
				public void addInt(int value) {
					dateColumns[colIndex].append(LocalDate.ofEpochDay(value));
					rowColumnsSet[colIndex] = true;
				}
			};
		}
		if(columnType == ColumnType.LOCAL_TIME) {
			final LogicalTypeAnnotation annotation = schemaType.getLogicalTypeAnnotation();
			if(annotation == null) {
				return new PrimitiveConverter() {
					@Override
					public void addInt(int value) {
						timeColumns[colIndex].append(LocalTime.ofNanoOfDay(MILLIS_TO_NANO * value));
						rowColumnsSet[colIndex] = true;
					}
					@Override
					public void addLong(long value) {
						timeColumns[colIndex].append(LocalTime.ofNanoOfDay(value));
						rowColumnsSet[colIndex] = true;
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
								timeColumns[colIndex].append(LocalTime.ofNanoOfDay(value * MICRO_TO_NANO));
								rowColumnsSet[colIndex] = true;
								break;
							case NANOS:
								timeColumns[colIndex].append(LocalTime.ofNanoOfDay(value));
								rowColumnsSet[colIndex] = true;
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
					timeColumns[colIndex].append(LocalTime.ofNanoOfDay(MILLIS_TO_NANO * value));
					rowColumnsSet[colIndex] = true;
				}
				@Override
				public void addLong(long value) {
					timeColumns[colIndex].append(LocalTime.ofNanoOfDay(value));
					rowColumnsSet[colIndex] = true;
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
						dateTimeColumns[colIndex].append(LocalDateTime.ofEpochSecond(epochSecond,
								(int)((value - (epochSecond * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
						rowColumnsSet[colIndex] = true;
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
								dateTimeColumns[colIndex].append(LocalDateTime.ofEpochSecond(epochSecondFromMicros,
										(int)(value - (epochSecondFromMicros * SECOND_TO_MICROS) * MICRO_TO_NANO), ZoneOffset.UTC));
								rowColumnsSet[colIndex] = true;
								break;
							case MILLIS:
								final long epochSecondFromMillis = value / SECOND_TO_MILLIS;
								dateTimeColumns[colIndex].append(LocalDateTime.ofEpochSecond(epochSecondFromMillis,
										(int)((value - (epochSecondFromMillis * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
								rowColumnsSet[colIndex] = true;
								break;
							case NANOS:
								final long epochSecondFromNanos = value / SECOND_TO_NANOS;
								dateTimeColumns[colIndex].append(LocalDateTime.ofEpochSecond(epochSecondFromNanos,
										(int)((value - (epochSecondFromNanos * SECOND_TO_NANOS))), ZoneOffset.UTC));
								rowColumnsSet[colIndex] = true;
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
					dateTimeColumns[colIndex].append(LocalDateTime.ofEpochSecond(epochSecond,
							(int)((value - (epochSecond * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
					rowColumnsSet[colIndex] = true;
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
		currentRownum++;
	}

	@Override
	public void end() {
		for(int i = 0; i < rowColumnsSet.length; i++) {
			if(!rowColumnsSet[i]) {
				table.column(i).appendMissing();
			} else {
				rowColumnsSet[i] = false;
			}
		}
	}

	public Row getCurrentRow() {
		if(this.currentRow == null) {
			this.currentRow = table.row(currentRownum);
		} else {
			currentRow.at(currentRownum);
		}
		return currentRow;
	}
	
	
}
