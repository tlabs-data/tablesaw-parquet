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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.LogicalTypeAnnotation.BsonLogicalTypeAnnotation;
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

import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions.ManageGroupsAs;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions.UnnanotatedBinaryAs;

public class TablesawReadSupport extends ReadSupport<Row> {

    private final TablesawParquetReadOptions options;
    private Table table = null;

    public TablesawReadSupport(final TablesawParquetReadOptions options) {
        super();
        this.options = options;
    }

    @Override
    public ReadContext init(final InitContext context) {
        final List<Type> keptFields = context.getFileSchema().getFields().stream()
            .filter(this::acceptFieldName)
            .filter(this::acceptFieldType)
            .collect(Collectors.toList());
        return new ReadContext(new MessageType(PARQUET_READ_SCHEMA, keptFields));
    }

    @Override
    public RecordMaterializer<Row> prepareForRead(final Configuration configuration,
            final Map<String, String> keyValueMetaData, final MessageType fileSchema, final ReadContext readContext) {
        final MessageType requestedSchema = readContext.getRequestedSchema();
        this.table = createTable(requestedSchema, this.options);
        return new TablesawRecordMaterializer(this.table, requestedSchema, this.options);
    }
    
    private boolean acceptFieldName(final Type type) {
        return this.options.hasColumn(type.getName());
    }
    
    private boolean acceptFieldType(final Type type) {
        if(type.isPrimitive() && !type.isRepetition(Repetition.REPEATED)) {
            return acceptSimplePrimitives(type);
        }
        return acceptGroupsAndRepeatedFields();
    }

    private boolean acceptGroupsAndRepeatedFields() {
        return this.options.getManageGroupsAs() != ManageGroupsAs.SKIP;
    }
    
    private boolean acceptSimplePrimitives(final Type type) {
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case FIXED_LEN_BYTE_ARRAY:
                if(type.getLogicalTypeAnnotation() == null) {
                    return this.options.getUnnanotatedBinaryAs() != UnnanotatedBinaryAs.SKIP;
                }
                return true;
            case BINARY:
                if(type.getLogicalTypeAnnotation() == null) {
                    return this.options.getUnnanotatedBinaryAs() != UnnanotatedBinaryAs.SKIP;
                }
                // Filtering out BSON
                return Optional.ofNullable(type.getLogicalTypeAnnotation())
                    .flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Boolean>() {
                        @Override
                        public Optional<Boolean> visit(final BsonLogicalTypeAnnotation bsonLogicalType) {
                            return Optional.of(Boolean.FALSE);
                        }
                    }))
                    .orElse(Boolean.TRUE);
            default:
                return true;
        }
    }
    
    private static Table createTable(final MessageType schema, final TablesawParquetReadOptions options) {
        final Table createdTable = Table.create(
            schema.getFields().stream()
            .map(f -> createColumn(f, options))
            .collect(Collectors.toList()));
        createdTable.setName(options.tableName());
        if(!options.getColumns().isEmpty()) {
            return createdTable.reorderColumns(options.getColumns()
                .toArray(new String[createdTable.columnCount()]));
        }
        return createdTable;
    }

    private static Column<?> createColumn(final Type field, final TablesawParquetReadOptions options) {
        final String name = field.getName();
        if (field.isPrimitive() && !field.isRepetition(Repetition.REPEATED)) {
            return createSimplePrimitiveColumn(name, field, options);
        }
        // Groups or repeated primitives are treated the same
        // treatment depends on manageGroupAs option
        switch (options.getManageGroupsAs()) {
            case ERROR:
                throw new UnsupportedOperationException("Column " + name + " is a group");
            case SKIP:
                throw new IllegalStateException("Skipped field still in schema");
            case TEXT:
                // CASCADE
            default:
                return TextColumn.create(name);
        }
    }

    private static Column<?> createSimplePrimitiveColumn(final String fieldName, final Type fieldType,
            final TablesawParquetReadOptions options) {
        switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return BooleanColumn.create(fieldName);
            case INT32:
                return Optional.ofNullable(fieldType.getLogicalTypeAnnotation())
                    .flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
                        @Override
                        public Optional<Column<?>> visit(final DateLogicalTypeAnnotation dateLogicalType) {
                            return Optional.of(DateColumn.create(fieldName));
                        }

                        @Override
                        public Optional<Column<?>> visit(final TimeLogicalTypeAnnotation timeLogicalType) {
                            return Optional.of(TimeColumn.create(fieldName));
                        }

                        @Override
                        public Optional<Column<?>> visit(final IntLogicalTypeAnnotation intLogicalType) {
                            return Optional.of(mustUseShortColumn(intLogicalType, options) ?
                                ShortColumn.create(fieldName) : IntColumn.create(fieldName));
                        }

                    }))
                    .orElseGet(() -> IntColumn.create(fieldName));
            case INT64:
                return Optional.ofNullable(fieldType.getLogicalTypeAnnotation())
                    .flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
                        @Override
                        public Optional<Column<?>> visit(final TimeLogicalTypeAnnotation timeLogicalType) {
                            return Optional.of(TimeColumn.create(fieldName));
                        }

                        @Override
                        public Optional<Column<?>> visit(final TimestampLogicalTypeAnnotation timestampLogicalType) {
                            return Optional.of(timestampLogicalType.isAdjustedToUTC() ?
                                InstantColumn.create(fieldName) : DateTimeColumn.create(fieldName));
                        }
                    }))
                    .orElseGet(() -> LongColumn.create(fieldName));
            case FLOAT:
                return options.isFloatColumnTypeUsed() ? FloatColumn.create(fieldName) : DoubleColumn.create(fieldName);
            case DOUBLE:
                return DoubleColumn.create(fieldName);
            case FIXED_LEN_BYTE_ARRAY:
                return Optional.ofNullable(fieldType.getLogicalTypeAnnotation())
                    .flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
                        @Override
                        public Optional<Column<?>> visit(final DecimalLogicalTypeAnnotation decimalLogicalType) {
                            return Optional.of(DoubleColumn.create(fieldName));
                        }
                    }))
                    .orElseGet(() -> StringColumn.create(fieldName));
            case INT96:
                return options.isConvertInt96ToTimestamp() ?
                    InstantColumn.create(fieldName) : StringColumn.create(fieldName);
            case BINARY:
                return Optional.ofNullable(fieldType.getLogicalTypeAnnotation())
                    .flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
                        @Override
                        public Optional<Column<?>> visit(final StringLogicalTypeAnnotation stringLogicalType) {
                            return Optional.of(StringColumn.create(fieldName));
                        }

                        @Override
                        public Optional<Column<?>> visit(final EnumLogicalTypeAnnotation enumLogicalType) {
                            return Optional.of(StringColumn.create(fieldName));
                        }

                        @Override
                        public Optional<Column<?>> visit(final JsonLogicalTypeAnnotation jsonLogicalType) {
                            return Optional.of(TextColumn.create(fieldName));
                        }

                        @Override
                        public Optional<Column<?>> visit(final DecimalLogicalTypeAnnotation decimalLogicalType) {
                            return Optional.of(DoubleColumn.create(fieldName));
                        }
                    }))
                    .orElseGet(() -> StringColumn.create(fieldName));
                default:
                    throw new IllegalStateException("Unknown field type " + fieldType.getName()
                        + ", column " + fieldName + " will be skipped");
        }
    }

    private static boolean mustUseShortColumn(final IntLogicalTypeAnnotation intLogicalType,
            final TablesawParquetReadOptions options) {
        return options.isShortColumnTypeUsed() && intLogicalType.getBitWidth() < 32;
    }

    public Table getTable() {
        return table;
    }
}
