package net.tlabs.tablesaw.parquet;

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
import tech.tablesaw.api.ShortColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.api.TextColumn;
import tech.tablesaw.api.TimeColumn;
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
  private final BooleanColumn[] booleanColumns;
  private final ShortColumn[] shortColumns;
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
    this.table = table;
    this.schema = internalCreateSchema(table);
    final int size = table.columnCount();
    booleanColumns = new BooleanColumn[size];
    shortColumns = new ShortColumn[size];
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
    for (int i = 0; i < size; i++) {
      fillColumnArrays(i, table.column(i).type());
    }
  }

  private void fillColumnArrays(final int i, final ColumnType columnType) {
    if (columnType == ColumnType.BOOLEAN) {
      booleanColumns[i] = table.booleanColumn(i);
    } else if (columnType == ColumnType.SHORT) {
      shortColumns[i] = table.shortColumn(i);
    } else if (columnType == ColumnType.INTEGER) {
      intColumns[i] = table.intColumn(i);
    } else if (columnType == ColumnType.LONG) {
      longColumns[i] = table.longColumn(i);
    } else if (columnType == ColumnType.FLOAT) {
      floatColumns[i] = table.floatColumn(i);
    } else if (columnType == ColumnType.DOUBLE) {
      doubleColumns[i] = table.doubleColumn(i);
    } else if (columnType == ColumnType.LOCAL_TIME) {
      timeColumns[i] = table.timeColumn(i);
    } else if (columnType == ColumnType.LOCAL_DATE) {
      dateColumns[i] = table.dateColumn(i);
    } else if (columnType == ColumnType.LOCAL_DATE_TIME) {
      dateTimeColumns[i] = table.dateTimeColumn(i);
    } else if (columnType == ColumnType.INSTANT) {
      instantColumns[i] = table.instantColumn(i);
    } else if (columnType == ColumnType.STRING) {
      stringColumns[i] = table.stringColumn(i);
    } else if (columnType == ColumnType.TEXT) {
      textColumns[i] = table.textColumn(i);
    }
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
    for (int i = 0; i < nbfields; i++) {
      final Column<?> column = table.column(i);
      if (!column.isMissing(rowNumber)) {
        final String fieldName = column.name();
        final ColumnType type = column.type();
        recordConsumer.startField(fieldName, i);
        if (type == ColumnType.BOOLEAN) {
          recordConsumer.addBoolean(booleanColumns[i].get(rowNumber));
        } else if (type == ColumnType.SHORT) {
          recordConsumer.addInteger(shortColumns[i].getShort(rowNumber));
        } else if (type == ColumnType.INTEGER) {
          recordConsumer.addInteger(intColumns[i].getInt(rowNumber));
        } else if (type == ColumnType.LONG) {
          recordConsumer.addLong(longColumns[i].getLong(rowNumber));
        } else if (type == ColumnType.FLOAT) {
          recordConsumer.addFloat(floatColumns[i].getFloat(rowNumber));
        } else if (type == ColumnType.DOUBLE) {
          recordConsumer.addDouble(doubleColumns[i].getDouble(rowNumber));
        } else if (type == ColumnType.STRING) {
          recordConsumer.addBinary(Binary.fromString(stringColumns[i].get(rowNumber)));
        } else if (type == ColumnType.TEXT) {
          recordConsumer.addBinary(Binary.fromString(textColumns[i].get(rowNumber)));
        } else if (type == ColumnType.LOCAL_DATE) {
          recordConsumer.addInteger(
              (int) PackedLocalDate.toEpochDay(dateColumns[i].getIntInternal(rowNumber)));
        } else if (type == ColumnType.LOCAL_TIME) {
          recordConsumer.addLong(
              PackedLocalTime.toNanoOfDay(timeColumns[i].getIntInternal(rowNumber)));
        } else if (type == ColumnType.LOCAL_DATE_TIME) {
          recordConsumer.addLong(
              dateTimeColumns[i].get(rowNumber).toInstant(ZoneOffset.UTC).toEpochMilli());
        } else if (type == ColumnType.INSTANT) {
          recordConsumer.addLong(instantColumns[i].get(rowNumber).toEpochMilli());
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
