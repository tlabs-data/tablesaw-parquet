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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions.UnnanotatedBinaryAs;
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
	public RecordMaterializer<Row> prepareForRead(final Configuration configuration,
			final Map<String, String> keyValueMetaData, final MessageType fileSchema, final ReadContext readContext) {
		this.table = createTable(fileSchema, this.options);
		this.table.setName(this.options.tableName());
		return new TablesawRecordMaterializer(this.table, fileSchema, this.options);
	}

	private Table createTable(final MessageType schema, final TablesawParquetReadOptions options) {
		return Table.create(schema.getFields().stream().map(f -> createColumn(f, options)).filter(Objects::nonNull)
				.collect(Collectors.toList()));
	}

	private Column<?> createColumn(final Type field, final TablesawParquetReadOptions options) {
		final String name = field.getName();
		if (field.isPrimitive() && !field.isRepetition(Repetition.REPEATED)) {
			switch (field.asPrimitiveType().getPrimitiveTypeName()) {
			case BOOLEAN:
				return BooleanColumn.create(name);
			case INT32:
				return Optional.ofNullable(field.getLogicalTypeAnnotation())
						.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
							@Override
							public Optional<Column<?>> visit(DateLogicalTypeAnnotation dateLogicalType) {
								return Optional.of(DateColumn.create(name));
							}

							@Override
							public Optional<Column<?>> visit(TimeLogicalTypeAnnotation timeLogicalType) {
								return Optional.of(TimeColumn.create(name));
							}

							@Override
							public Optional<Column<?>> visit(IntLogicalTypeAnnotation intLogicalType) {
								if (intLogicalType.getBitWidth() < 32 && options.isShortColumnTypeUsed()) {
									return Optional.of(ShortColumn.create(name));
								}
								return Optional.of(IntColumn.create(name));
							}
						})).orElseGet(() -> IntColumn.create(name));
			case INT64:
				return Optional.ofNullable(field.getLogicalTypeAnnotation())
						.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
							@Override
							public Optional<Column<?>> visit(TimeLogicalTypeAnnotation timeLogicalType) {
								return Optional.of(TimeColumn.create(name));
							}

							@Override
							public Optional<Column<?>> visit(TimestampLogicalTypeAnnotation timestampLogicalType) {
								if (timestampLogicalType.isAdjustedToUTC()) {
									return Optional.of(InstantColumn.create(name));
								}
								return Optional.of(DateTimeColumn.create(name));
							}
						})).orElseGet(() -> LongColumn.create(name));
			case FLOAT:
				return options.isFloatColumnTypeUsed() ? FloatColumn.create(name) : DoubleColumn.create(name);
			case DOUBLE:
				return DoubleColumn.create(name);
			case FIXED_LEN_BYTE_ARRAY:
				return Optional.ofNullable(field.getLogicalTypeAnnotation())
						.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
							@Override
							public Optional<Column<?>> visit(DecimalLogicalTypeAnnotation decimalLogicalType) {
								return Optional.of(DoubleColumn.create(name));
							}
						})).orElseGet(() -> options.getUnnanotatedBinaryAs() == UnnanotatedBinaryAs.SKIP ? null
								: StringColumn.create(name));
			case INT96:
				if (options.isConvertInt96ToTimestamp()) {
					return InstantColumn.create(name);
				}
				return StringColumn.create(name);
			case BINARY:
				// Filtering out BSON
				if (Optional.ofNullable(field.getLogicalTypeAnnotation())
						.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Boolean>() {
							@Override
							public Optional<Boolean> visit(BsonLogicalTypeAnnotation bsonLogicalType) {
								return Optional.of(Boolean.TRUE);
							}
						})).isPresent()) {
					return null;
				}
				return Optional.ofNullable(field.getLogicalTypeAnnotation())
						.flatMap(a -> a.accept(new LogicalTypeAnnotationVisitor<Column<?>>() {
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

							@Override
							public Optional<Column<?>> visit(DecimalLogicalTypeAnnotation decimalLogicalType) {
								return Optional.of(DoubleColumn.create(name));
							}
						})).orElseGet(() -> options.unnanotatedBinaryAs == UnnanotatedBinaryAs.SKIP ? null
								: StringColumn.create(name));
			}
		}
		// Groups or repeated primitives are treated the same
		// treatment depends on manageGroupAs option
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
