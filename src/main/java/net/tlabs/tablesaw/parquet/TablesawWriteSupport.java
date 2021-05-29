package net.tlabs.tablesaw.parquet;

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

public class TablesawWriteSupport extends WriteSupport<Row> {

  private static final String WRITE_SUPPORT_NAME = "tech.tablesaw";
  private static final Map<ColumnType, PrimitiveTypeName> PRIMITIVE_MAPPING;
  private static final Map<ColumnType, LogicalTypeAnnotation> ANNOTATION_MAPPING;
  private final TableProxy proxy;
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
    ANNOTATION_MAPPING.put(ColumnType.SHORT, LogicalTypeAnnotation.intType(16, true));
    ANNOTATION_MAPPING.put(ColumnType.LOCAL_DATE, LogicalTypeAnnotation.dateType());
    ANNOTATION_MAPPING.put(
        ColumnType.LOCAL_TIME, LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS));
    ANNOTATION_MAPPING.put(
        ColumnType.INSTANT, LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS));
    ANNOTATION_MAPPING.put(
        ColumnType.LOCAL_DATE_TIME, LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS));
    ANNOTATION_MAPPING.put(ColumnType.STRING, LogicalTypeAnnotation.stringType());
    ANNOTATION_MAPPING.put(ColumnType.TEXT, LogicalTypeAnnotation.stringType());
  }

  public TablesawWriteSupport(final Table table) {
    super();
    this.schema = internalCreateSchema(table);
    this.proxy = new TableProxy(table);
  }

  public static MessageType createSchema(final Table table) {
    return internalCreateSchema(table);
  }

  private static MessageType internalCreateSchema(final Table table) {
    final List<Type> fields = new ArrayList<>(table.columnCount());
    for (final Column<?> column : table.columns()) {
      fields.add(createType(column));
    }
    final String name = table.name();
    return new MessageType(name == null ? "message" : name, fields);
  }

  private static Type createType(final Column<?> column) {
    final ColumnType type = column.type();
    return Types.optional(PRIMITIVE_MAPPING.get(type))
        .as(ANNOTATION_MAPPING.get(type))
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
  public void write(final Row row) {
    final int rowNumber = row.getRowNumber();
    recordConsumer.startMessage();
    final int nbfields = schema.getFieldCount();
    for (int colIndex = 0; colIndex < nbfields; colIndex++) {
      final Column<?> column = proxy.column(colIndex);
      if (!column.isMissing(rowNumber)) {
        final String fieldName = column.name();
        final ColumnType type = column.type();
        recordConsumer.startField(fieldName, colIndex);
        if (type == ColumnType.BOOLEAN) {
          recordConsumer.addBoolean(proxy.getBoolean(colIndex, rowNumber));
        } else if (type == ColumnType.SHORT) {
          recordConsumer.addInteger(proxy.getShort(colIndex, rowNumber));
        } else if (type == ColumnType.INTEGER) {
          recordConsumer.addInteger(proxy.getInt(colIndex, rowNumber));
        } else if (type == ColumnType.LONG) {
          recordConsumer.addLong(proxy.getLong(colIndex, rowNumber));
        } else if (type == ColumnType.FLOAT) {
          recordConsumer.addFloat(proxy.getFloat(colIndex, rowNumber));
        } else if (type == ColumnType.DOUBLE) {
          recordConsumer.addDouble(proxy.getDouble(colIndex, rowNumber));
        } else if (type == ColumnType.STRING) {
          recordConsumer.addBinary(Binary.fromString(proxy.getString(colIndex, rowNumber)));
        } else if (type == ColumnType.TEXT) {
          recordConsumer.addBinary(Binary.fromString(proxy.getText(colIndex, rowNumber)));
        } else if (type == ColumnType.LOCAL_DATE) {
          recordConsumer.addInteger(proxy.getDateToEpochDay(colIndex, rowNumber));
        } else if (type == ColumnType.LOCAL_TIME) {
          recordConsumer.addLong(proxy.getTimeToNanoOfDay(colIndex, rowNumber));
        } else if (type == ColumnType.LOCAL_DATE_TIME) {
          recordConsumer.addLong(proxy.getDateTimeToEpochMilli(colIndex, rowNumber));
        } else if (type == ColumnType.INSTANT) {
          recordConsumer.addLong(proxy.getInstantToEpochMilli(colIndex, rowNumber));
        } else {
          // This should not happen
          throw new UnsupportedOperationException("Unsupported ColumnType: " + type);
        }
        recordConsumer.endField(fieldName, colIndex);
      }
    }
    recordConsumer.endMessage();
  }

  @Override
  public String getName() {
    return WRITE_SUPPORT_NAME;
  }
}
