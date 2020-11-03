package tech.tablesaw.io.parquet;

import java.io.IOException;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

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
import tech.tablesaw.columns.booleans.BooleanColumnType;
import tech.tablesaw.columns.dates.DateColumnType;
import tech.tablesaw.columns.datetimes.DateTimeColumnType;
import tech.tablesaw.columns.instant.InstantColumnType;
import tech.tablesaw.columns.numbers.DoubleColumnType;
import tech.tablesaw.columns.numbers.FloatColumnType;
import tech.tablesaw.columns.numbers.IntColumnType;
import tech.tablesaw.columns.numbers.LongColumnType;
import tech.tablesaw.columns.strings.StringColumnType;
import tech.tablesaw.columns.strings.TextColumnType;
import tech.tablesaw.columns.times.TimeColumnType;
import tech.tablesaw.io.DataReader;
import tech.tablesaw.io.ReaderRegistry;
import tech.tablesaw.io.Source;

public class TablesawParquetReader implements DataReader<TablesawParquetReadOptions> {

	private static final long SECOND_TO_MILLIS = 1_000l;

	private static final long SECOND_TO_MICROS = 1_000_000l;

	private static final long SECOND_TO_NANOS = 1_000_000_000l;

	private static final long MICRO_TO_NANO = 1_000l;

	private static final long MILLIS_TO_MICRO = 1_000l;

	private static final long MILLIS_TO_NANO = 1_000_000l;

	private static final TablesawParquetReader INSTANCE = new TablesawParquetReader();
	
	private static final Map<ColumnType, TablesawGroupConverter> MAPPER = new HashMap<>();
	
	private static String rawBytesToHexString(final byte[] bytes) {
		final String[] hexBytes = new String[bytes.length];
		for(int i = 0; i < bytes.length; i++) {
			hexBytes[i] = String.format("%02X", bytes[i]);
		}
		return String.join(" ", hexBytes);
	}

	static {
		register(Table.defaultReaderRegistry);
		MAPPER.put(BooleanColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processBoolean(final Row row, final String fieldName, final boolean value) {
				row.setBoolean(fieldName, value);
			}
		});
		MAPPER.put(IntColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processInteger(final Row row, final String fieldName, final int value,
					final LogicalTypeAnnotation annotation) {
				row.setInt(fieldName, value);
			}
		});
		MAPPER.put(LongColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processLong(final Row row, final String fieldName, final long value,
					final LogicalTypeAnnotation annotation) {
				row.setLong(fieldName, value);
			}
		});
		MAPPER.put(FloatColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processFloat(final Row row, final String fieldName, final float value) {
				row.setFloat(fieldName, value);
			}
		});
		MAPPER.put(DoubleColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processDouble(final Row row, final String fieldName, final double value) {
				row.setDouble(fieldName, value);
			}
		});
		MAPPER.put(StringColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processString(final Row row, final String fieldName, final String value) {
				row.setString(fieldName, value);
			}
			@Override
			public void processBinary(final Row row, final String fieldName, final Binary value,
					final LogicalTypeAnnotation annotation) {
				if(annotation == null) {
				    processString(row, fieldName, rawBytesToHexString(value.getBytes()));
				} else if(annotation instanceof IntervalLogicalTypeAnnotation) {
				    Preconditions.checkArgument(value.length() == 12, "Must be 12 bytes");
				    final ByteBuffer buf = value.toByteBuffer();
				    buf.order(ByteOrder.LITTLE_ENDIAN);
					processString(row, fieldName, Period.ofMonths(buf.getInt()).plusDays(buf.getInt()).toString()
							+ Duration.ofMillis(buf.getInt()).toString().substring(1));
				} // TODO: identify UUIDs
				else {
					processString(row, fieldName, rawBytesToHexString(value.getBytes()));
				}
			}
			@Override
			public void processInt96(final Row row, final String fieldName, final Binary value,
					final LogicalTypeAnnotation annotation) {
				processString(row, fieldName, rawBytesToHexString(value.getBytes()));
			}
		});
		MAPPER.put(TextColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processString(final Row row, final String fieldName, final String value) {
				row.setText(fieldName, value);
			}
		});
		MAPPER.put(InstantColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processLong(final Row row, final String fieldName, final long value,
					final LogicalTypeAnnotation annotation) {
				if(annotation != null && annotation instanceof TimestampLogicalTypeAnnotation) {
					final TimestampLogicalTypeAnnotation tsannotation = (TimestampLogicalTypeAnnotation)annotation;
					switch(tsannotation.getUnit()) {
					case MICROS:
						final long millisFromMicro = value / MILLIS_TO_MICRO;
						row.setInstant(fieldName,
								Instant.ofEpochMilli(millisFromMicro)
								.plus(value - millisFromMicro * MILLIS_TO_MICRO, ChronoUnit.MICROS));
						break;
					case MILLIS:
						row.setInstant(fieldName, Instant.ofEpochMilli(value));
						break;
					case NANOS:
						final long millisFromNanos = value / MILLIS_TO_NANO;
						row.setInstant(fieldName,
								Instant.ofEpochMilli(millisFromNanos).plusNanos(value - millisFromNanos * MILLIS_TO_NANO));
						break;
					default:
						throw new UnsupportedOperationException(
								"This should never happen: TimeUnit is neither MILLIS, MICROS or NANOS in Timestamp");
					}
				} else {
					row.setInstant(fieldName, Instant.ofEpochMilli(value));
				}
			}
			@Override
			public void processInt96(final Row row, final String fieldName, final Binary value,
					final LogicalTypeAnnotation annotation) {
				final NanoTime nanoTime = NanoTime.fromBinary(value);
				final LocalDate date = LocalDate.ofEpochDay(0).with(JulianFields.JULIAN_DAY, nanoTime.getJulianDay());
				row.setInstant(fieldName, ZonedDateTime.of(date.atStartOfDay(), ZoneOffset.UTC).toInstant()
						.plus(nanoTime.getTimeOfDayNanos(), ChronoUnit.NANOS));
			}
		});
		MAPPER.put(DateColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processInteger(final Row row, final String fieldName, final int value,
					final LogicalTypeAnnotation annotation) {
				row.setDate(fieldName, LocalDate.ofEpochDay(value));
			}
		});
		MAPPER.put(TimeColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processInteger(final Row row, final String fieldName, final int value,
					final LogicalTypeAnnotation annotation) {
				row.setTime(fieldName, LocalTime.ofNanoOfDay(MILLIS_TO_NANO * value));
			}
			@Override
			public void processLong(final Row row, final String fieldName, final long value,
					final LogicalTypeAnnotation annotation) {
				if (annotation != null && annotation instanceof TimeLogicalTypeAnnotation) {
					final TimeLogicalTypeAnnotation tannotation = (TimeLogicalTypeAnnotation)annotation;
					switch(tannotation.getUnit()) {
					case MICROS:
						row.setTime(fieldName, LocalTime.ofNanoOfDay(value * MICRO_TO_NANO));
						break;
					case NANOS:
						row.setTime(fieldName,LocalTime.ofNanoOfDay(value));
						break;
					default:
						throw new UnsupportedOperationException(
								"This should never happen: TimeUnit is neither MICROS or NANOS in Int64 Time");					
					}
				} else {
					row.setTime(fieldName,LocalTime.ofNanoOfDay(value));
				}
			}
		});
		MAPPER.put(DateTimeColumnType.instance(), new TablesawGroupConverter() {
			@Override
			public void processLong(final Row row, final String fieldName, final long value,
					final LogicalTypeAnnotation annotation) {
				if (annotation != null && annotation instanceof TimestampLogicalTypeAnnotation) {
					final TimestampLogicalTypeAnnotation tsannotation = (TimestampLogicalTypeAnnotation)annotation;
					switch(tsannotation.getUnit()) {
					case MICROS:
						final long epochSecondFromMicros = value / SECOND_TO_MICROS;
						row.setDateTime(fieldName, LocalDateTime.ofEpochSecond(epochSecondFromMicros,
								(int)(value - (epochSecondFromMicros * SECOND_TO_MICROS) * MICRO_TO_NANO), ZoneOffset.UTC));
						break;
					case MILLIS:
						final long epochSecondFromMillis = value / SECOND_TO_MILLIS;
						row.setDateTime(fieldName, LocalDateTime.ofEpochSecond(epochSecondFromMillis,
								(int)((value - (epochSecondFromMillis * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
						break;
					case NANOS:
						final long epochSecondFromNanos = value / SECOND_TO_NANOS;
						row.setDateTime(fieldName, LocalDateTime.ofEpochSecond(epochSecondFromNanos,
								(int)((value - (epochSecondFromNanos * SECOND_TO_NANOS))), ZoneOffset.UTC));
						break;
					default:
						throw new UnsupportedOperationException(
								"This should never happen: TimeUnit is neither MILLIS, MICROS or NANOS in DateTime");
					}					
				} else {
					final long epochSecond = value / SECOND_TO_MILLIS;
					row.setDateTime(fieldName, LocalDateTime.ofEpochSecond(epochSecond,
							(int)((value - (epochSecond * SECOND_TO_MILLIS)) * MILLIS_TO_NANO), ZoneOffset.UTC));
				}
			}
		});
	}

	public static void register(ReaderRegistry registry) {
		registry.registerExtension("parquet", INSTANCE);
		registry.registerMimeType("parquet", INSTANCE); // TODO: find parquet mime type
		registry.registerOptions(TablesawParquetReadOptions.class, INSTANCE);
	}

	@Override
	public Table read(final Source source) throws IOException {
		return read(TablesawParquetReadOptions.builder(source).build());
	}

	@Override
	public Table read(final TablesawParquetReadOptions options) throws IOException {
		final HadoopInputFile hif = HadoopInputFile.fromPath(
				new Path(options.source().file().getAbsolutePath()),
				new Configuration());
		final ParquetReadOptions opts = HadoopReadOptions.builder(hif.getConfiguration())
				.withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build();
		try (final ParquetFileReader reader = ParquetFileReader.open(hif, opts)) {
			final MessageType schema = reader.getFooter().getFileMetaData().getSchema();
			final Table table = createTable(schema, options);
			table.setName(options.tableName());
			while (true) {
				final PageReadStore pages = reader.readNextRowGroup();
				if (pages == null) return table;
				final long rows = pages.getRowCount();
				final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
				final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
				for (int i = 0; i < rows; i++) {
					final Group group = recordReader.read();
					fillRow(table.appendRow(), group, options);
				}
			}
		}
	}

	private void fillRow(final Row row, final Group group, final TablesawParquetReadOptions options) {
		final int fieldCount = group.getType().getFieldCount();
		for (int field = 0; field < fieldCount; field++) {
			final Type fieldType = group.getType().getType(field);
			final String fieldName = fieldType.getName();
			if(!row.hasColumn(fieldName)) {
				continue;
			}
			final int valueCount = group.getFieldRepetitionCount(field);
			if(valueCount == 0) {
				row.setMissing(fieldName);
				return;
			}
			final ColumnType columnType = row.getColumnType(fieldName);
			final TablesawGroupConverter tablesawGroupConverter = MAPPER.get(columnType);
			if (valueCount > 1 || !fieldType.isPrimitive()) {
				tablesawGroupConverter.processString(row, fieldName, group.toString());
				return;
			}
			final LogicalTypeAnnotation annotation = fieldType.getLogicalTypeAnnotation();
			switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
			case BOOLEAN:
				tablesawGroupConverter.processBoolean(row, fieldName, group.getBoolean(field, 0));
				break;
			case INT32:
				tablesawGroupConverter.processInteger(row, fieldName, group.getInteger(field, 0), annotation);
				break;
			case INT64:
				tablesawGroupConverter.processLong(row, fieldName, group.getLong(field, 0), annotation);
				break;
			case FLOAT:
				tablesawGroupConverter.processFloat(row, fieldName, group.getFloat(field, 0));
				break;
			case DOUBLE:
				tablesawGroupConverter.processDouble(row, fieldName, group.getDouble(field, 0));
				break;
			case FIXED_LEN_BYTE_ARRAY:
				tablesawGroupConverter.processBinary(row, fieldName, group.getBinary(field, 0), annotation);
				break;
			case INT96:
				tablesawGroupConverter.processInt96(row, fieldName, group.getInt96(field, 0), annotation);
				break;
			case BINARY:
				if((annotation == null && options.unnanotatedBinaryAsString()) 
						|| annotation instanceof StringLogicalTypeAnnotation
						|| annotation instanceof EnumLogicalTypeAnnotation) {
					tablesawGroupConverter.processString(row, fieldName, group.getString(field, 0));
				} else {
					tablesawGroupConverter.processBinary(row, fieldName, group.getBinary(field, 0), annotation);
				}
				break;
			default:
				break;
			}
		}
	}

	private Table createTable(final MessageType schema, final TablesawParquetReadOptions options) {
		return Table.create(
				schema.getFields().stream()
				.map(f -> createColumn(f, options))
				.filter(c -> c != null)
				.collect(Collectors.toList())
				);
	}

	private Column<?> createColumn(final Type field, final TablesawParquetReadOptions options) {
		final String name = field.getName();
		if(field.isPrimitive() && !field.isRepetition(Repetition.REPEATED)) {
			final LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
			switch (field.asPrimitiveType().getPrimitiveTypeName()) {
			case BOOLEAN:
				return BooleanColumn.create(name);
			case INT32:
				return annotation == null ? IntColumn.create(name) :
					annotation.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
					@Override
					public Optional<Column<?>> visit(DateLogicalTypeAnnotation dateLogicalType) {
						return Optional.of(DateColumn.create(name));
					}
					@Override
					public Optional<Column<?>> visit(TimeLogicalTypeAnnotation timeLogicalType) {
						return Optional.of(TimeColumn.create(name));
					}
				}).orElse(IntColumn.create(name));
			case INT64:
				return annotation == null ? LongColumn.create(name) :
					annotation.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
					@Override
					public Optional<Column<?>> visit(TimeLogicalTypeAnnotation timeLogicalType) {
						return Optional.of(TimeColumn.create(name));
					}
					@Override
					public Optional<Column<?>> visit(TimestampLogicalTypeAnnotation timestampLogicalType) {
						if(timestampLogicalType.isAdjustedToUTC()) {
							return Optional.of(InstantColumn.create(name));
						}
						return Optional.of(DateTimeColumn.create(name));
					}
				}).orElse(LongColumn.create(name));
			case FLOAT:
				return FloatColumn.create(name);
			case DOUBLE:
				return DoubleColumn.create(name); 
			case FIXED_LEN_BYTE_ARRAY:
				return StringColumn.create(name);
			case INT96:
				if(options.isConvertInt96ToTimestamp()) {
					return InstantColumn.create(name);
				}
				return StringColumn.create(name);
			case BINARY:
				return annotation == null ? StringColumn.create(name) :
					annotation.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
					@Override
					public Optional<Column<?>> visit(StringLogicalTypeAnnotation stringLogicalType) {
						return Optional.of(StringColumn.create(name));
					}
					@Override
					public Optional<Column<?>> visit(EnumLogicalTypeAnnotation enumLogicalType) {
						return Optional.of(StringColumn.create(name));
					}
					@Override
					public Optional<Column<?>> visit(JsonLogicalTypeAnnotation jsonLogicalType) {
						return Optional.of(TextColumn.create(name));
					}
				}).orElse(StringColumn.create(name));
			}
		}
		switch(options.getManageGroupsAs()) {
		case ERROR:
			throw new UnsupportedOperationException("Column " + name + " is a group");
		case SKIP:
			return null;
		case TEXT:
			// CASCADE
		default:
			return TextColumn.create(name);
		}
	}

	public interface TablesawGroupConverter {
		default void processBoolean(final Row row, final String fieldName, final boolean value) {
			throw new UnsupportedOperationException(
					"No conversion from boolean to " + row.getColumnType(fieldName).getPrinterFriendlyName());
		}
		default void processInteger(final Row row, final String fieldName, final int value,
				final LogicalTypeAnnotation annotation) {
			throw new UnsupportedOperationException(
					"No conversion from int to " + row.getColumnType(fieldName).getPrinterFriendlyName());
		}
		default void processLong(final Row row, final String fieldName, final long value,
				final LogicalTypeAnnotation annotation) {
			throw new UnsupportedOperationException(
					"No conversion from long to " + row.getColumnType(fieldName).getPrinterFriendlyName());
		}
		default void processFloat(final Row row, final String fieldName, final float value) {
			throw new UnsupportedOperationException(
					"No conversion from float to " + row.getColumnType(fieldName).getPrinterFriendlyName());
		}
		default void processDouble(final Row row, final String fieldName, final double value) {
			throw new UnsupportedOperationException(
					"No conversion from double to " + row.getColumnType(fieldName).getPrinterFriendlyName());
		}
		default void processString(final Row row, final String fieldName, final String value) {
			throw new UnsupportedOperationException(
					"No conversion from String to " + row.getColumnType(fieldName).getPrinterFriendlyName());
		}
		default void processBinary(final Row row, final String fieldName, final Binary value,
				final LogicalTypeAnnotation annotation) {
			throw new UnsupportedOperationException(
					"No conversion from Binary (" + annotation.toString() + ") to " + row.getColumnType(fieldName).getPrinterFriendlyName());
		}
		default void processInt96(final Row row, final String fieldName, final Binary binary,
				final LogicalTypeAnnotation annotation) {
			throw new UnsupportedOperationException(
					"No conversion from Int96 to " + row.getColumnType(fieldName).getPrinterFriendlyName());
		}
	}
}
