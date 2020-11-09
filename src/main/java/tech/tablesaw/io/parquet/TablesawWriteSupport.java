package tech.tablesaw.io.parquet;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import tech.tablesaw.columns.dates.PackedLocalDate;
import tech.tablesaw.columns.times.PackedLocalTime;

public class TablesawWriteSupport extends WriteSupport<Row> {

	private static final String WRITE_SUPPORT_NAME = "tech.tablesaw";
	private static final Map<ColumnType, PrimitiveTypeName> PRIMITIVE_MAPPING;
	private static final Map<ColumnType, LogicalTypeAnnotation> ANNOTATION_MAPPING;
	private final Table table;
	private final MessageType schema;
	private RecordConsumer recordConsumer;

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
		PRIMITIVE_MAPPING.put(ColumnType.LOCAL_TIME, PrimitiveTypeName.INT64);
		PRIMITIVE_MAPPING.put(ColumnType.LOCAL_DATE_TIME, PrimitiveTypeName.INT64);
		PRIMITIVE_MAPPING.put(ColumnType.STRING, PrimitiveTypeName.BINARY);
		PRIMITIVE_MAPPING.put(ColumnType.TEXT, PrimitiveTypeName.BINARY);
		ANNOTATION_MAPPING = new HashMap<>();
		ANNOTATION_MAPPING.put(ColumnType.LOCAL_DATE, LogicalTypeAnnotation.dateType());
		ANNOTATION_MAPPING.put(ColumnType.LOCAL_TIME, LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS));
		ANNOTATION_MAPPING.put(ColumnType.INSTANT, LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS));
		ANNOTATION_MAPPING.put(ColumnType.LOCAL_DATE_TIME, LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS));
		ANNOTATION_MAPPING.put(ColumnType.STRING, LogicalTypeAnnotation.stringType());
		ANNOTATION_MAPPING.put(ColumnType.TEXT, LogicalTypeAnnotation.stringType());
	}
	
	public TablesawWriteSupport(final Table table) {
		super();
		this.table = table;
		this.schema = internalCreateSchema(table);
	}

	public static MessageType createSchema(final Table table) {
		return internalCreateSchema(table);
	}
	
	private static MessageType internalCreateSchema(final Table table) {
		final List<Type> fields = new ArrayList<>(table.columnCount());
		for(final Column<?> column : table.columns()) {
			fields.add(createType(column));
		}
		return new MessageType(table.name(), fields);
	}

	private static Type createType(final Column<?> column) {
		final ColumnType type = column.type();
		return Types.optional(PRIMITIVE_MAPPING.get(type))
				.as(ANNOTATION_MAPPING.get(column.type()))
				.named(column.name());
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
	public void write(final Row record) {
		final int rowNumber = record.getRowNumber();
		recordConsumer.startMessage();
		final int nbfields = schema.getFieldCount();
		for(int i = 0; i < nbfields; i++) {
			final Column<?> column = table.column(i);
			if(!column.isMissing(rowNumber)) {
				final String fieldName = column.name();
				final ColumnType type = column.type();
				recordConsumer.startField(fieldName, i);
				if(type == ColumnType.BOOLEAN) {
					recordConsumer.addBoolean(table.booleanColumn(i).get(rowNumber));
				} else if(type == ColumnType.SHORT) {
					recordConsumer.addInteger(table.shortColumn(i).getShort(rowNumber));
				} else if(type == ColumnType.INTEGER) {
					recordConsumer.addInteger(table.intColumn(i).getInt(rowNumber));
				} else if(type == ColumnType.LONG) {
					recordConsumer.addLong(table.longColumn(i).getLong(rowNumber));
				} else if(type == ColumnType.FLOAT) {
					recordConsumer.addFloat(table.floatColumn(i).getFloat(rowNumber));
				} else if(type == ColumnType.DOUBLE) {
					recordConsumer.addDouble(table.doubleColumn(i).getDouble(rowNumber));
				} else if(type == ColumnType.STRING) {
					recordConsumer.addBinary(Binary.fromString(table.stringColumn(i).get(rowNumber)));
				} else if(type == ColumnType.TEXT) {
					recordConsumer.addBinary(Binary.fromString(table.textColumn(i).get(rowNumber)));
				} else if(type == ColumnType.LOCAL_DATE) {
					recordConsumer.addInteger((int) PackedLocalDate.toEpochDay(table.dateColumn(i).getIntInternal(rowNumber)));
				} else if(type == ColumnType.LOCAL_TIME) {
					recordConsumer.addLong(PackedLocalTime.toNanoOfDay(table.timeColumn(i).getIntInternal(rowNumber)));
				} else if(type == ColumnType.LOCAL_DATE_TIME) {
					recordConsumer.addLong(table.dateTimeColumn(i).get(rowNumber).toInstant(ZoneOffset.UTC).toEpochMilli());
				} else if(type == ColumnType.INSTANT) {
					recordConsumer.addLong(table.instantColumn(i).get(rowNumber).toEpochMilli());
				} else {
					// This should not happen
					throw new UnsupportedOperationException("Unsupported ColumnType: " + type);
				}
				recordConsumer.endField(fieldName, i);
			}
		}
		recordConsumer.endMessage();
	}

	@Override
	public String getName() {
		return WRITE_SUPPORT_NAME;
	}

}
