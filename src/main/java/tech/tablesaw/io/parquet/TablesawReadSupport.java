package tech.tablesaw.io.parquet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
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
import tech.tablesaw.api.ShortColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.api.TextColumn;
import tech.tablesaw.api.TimeColumn;
import tech.tablesaw.columns.Column;

public class TablesawReadSupport extends ReadSupport<Row> {

  private final TablesawParquetReadOptions options;
  private Table table = null;

  public TablesawReadSupport(final TablesawParquetReadOptions options) {
    super();
    this.options = options;
  }

  @Override
  public ReadContext init(final InitContext context) {
    return new ReadContext(context.getFileSchema());
  }

  @Override
  public RecordMaterializer<Row> prepareForRead(
      final Configuration configuration,
      final Map<String, String> keyValueMetaData,
      final MessageType fileSchema,
      final ReadContext readContext) {
    this.table = createTable(fileSchema, this.options);
    this.table.setName(this.options.tableName());
    return new TablesawRecordMaterializer(this.table, fileSchema, this.options);
  }

  private Table createTable(final MessageType schema, final TablesawParquetReadOptions options) {
    return Table.create(
        schema.getFields().stream()
            .map(f -> createColumn(f, options))
            .filter(c -> c != null)
            .collect(Collectors.toList()));
  }

  private Column<?> createColumn(final Type field, final TablesawParquetReadOptions options) {
    final String name = field.getName();
    final List<ColumnType> columnTypesToDetect = options.columnTypesToDetect();
    if (field.isPrimitive() && !field.isRepetition(Repetition.REPEATED)) {
      final LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
      switch (field.asPrimitiveType().getPrimitiveTypeName()) {
        case BOOLEAN:
          return BooleanColumn.create(name);
        case INT32:
          return annotation == null
              ? IntColumn.create(name)
              : annotation
                  .accept(
                      new LogicalTypeAnnotationVisitor<Column<?>>() {
                        @Override
                        public Optional<Column<?>> visit(
                            DateLogicalTypeAnnotation dateLogicalType) {
                          return Optional.of(DateColumn.create(name));
                        }

                        @Override
                        public Optional<Column<?>> visit(
                            TimeLogicalTypeAnnotation timeLogicalType) {
                          return Optional.of(TimeColumn.create(name));
                        }

                        @Override
                        public Optional<Column<?>> visit(IntLogicalTypeAnnotation intLogicalType) {
                          if (intLogicalType.getBitWidth() < 32
                              && columnTypesToDetect.contains(ColumnType.SHORT)) {
                            return Optional.of(ShortColumn.create(name));
                          }
                          return Optional.of(IntColumn.create(name));
                        }
                      })
                  .orElse(IntColumn.create(name));
        case INT64:
          return annotation == null
              ? LongColumn.create(name)
              : annotation
                  .accept(
                      new LogicalTypeAnnotationVisitor<Column<?>>() {
                        @Override
                        public Optional<Column<?>> visit(
                            TimeLogicalTypeAnnotation timeLogicalType) {
                          return Optional.of(TimeColumn.create(name));
                        }

                        @Override
                        public Optional<Column<?>> visit(
                            TimestampLogicalTypeAnnotation timestampLogicalType) {
                          if (timestampLogicalType.isAdjustedToUTC()) {
                            return Optional.of(InstantColumn.create(name));
                          }
                          return Optional.of(DateTimeColumn.create(name));
                        }
                      })
                  .orElse(LongColumn.create(name));
        case FLOAT:
          return columnTypesToDetect.contains(ColumnType.FLOAT)
              ? FloatColumn.create(name)
              : DoubleColumn.create(name);
        case DOUBLE:
          return DoubleColumn.create(name);
        case FIXED_LEN_BYTE_ARRAY:
          return annotation == null
              ? StringColumn.create(name)
              : annotation
                  .accept(
                      new LogicalTypeAnnotationVisitor<Column<?>>() {
                        @Override
                        public Optional<Column<?>> visit(
                            DecimalLogicalTypeAnnotation decimalLogicalType) {
                          return Optional.of(DoubleColumn.create(name));
                        }
                      })
                  .orElse(StringColumn.create(name));
        case INT96:
          if (options.isConvertInt96ToTimestamp()) {
            return InstantColumn.create(name);
          }
          return StringColumn.create(name);
        case BINARY:
          return annotation == null
              ? StringColumn.create(name)
              : annotation
                  .accept(
                      new LogicalTypeAnnotationVisitor<Column<?>>() {
                        @Override
                        public Optional<Column<?>> visit(
                            StringLogicalTypeAnnotation stringLogicalType) {
                          return Optional.of(StringColumn.create(name));
                        }

                        @Override
                        public Optional<Column<?>> visit(
                            EnumLogicalTypeAnnotation enumLogicalType) {
                          return Optional.of(StringColumn.create(name));
                        }

                        @Override
                        public Optional<Column<?>> visit(
                            JsonLogicalTypeAnnotation jsonLogicalType) {
                          return Optional.of(TextColumn.create(name));
                        }

                        @Override
                        public Optional<Column<?>> visit(
                            DecimalLogicalTypeAnnotation decimalLogicalType) {
                          return Optional.of(DoubleColumn.create(name));
                        }
                      })
                  .orElse(StringColumn.create(name));
      }
    }
    switch (options.getManageGroupsAs()) {
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

  public Table getTable() {
    return table;
  }
}
