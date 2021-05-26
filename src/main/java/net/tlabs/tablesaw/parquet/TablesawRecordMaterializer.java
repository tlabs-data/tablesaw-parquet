package net.tlabs.tablesaw.parquet;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

public class TablesawRecordMaterializer extends RecordMaterializer<Row> {

  private final TablesawRecordConverter root;

  public TablesawRecordMaterializer(
      final Table table, final MessageType fileSchema, final TablesawParquetReadOptions options) {
    super();
    this.root = new TablesawRecordConverter(table, fileSchema, options);
  }

  @Override
  public Row getCurrentRecord() {
    return root.getCurrentRow();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
