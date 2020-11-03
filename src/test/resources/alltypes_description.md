From https://github.com/apache/parquet-testing

```
message schema {
  optional int32 id;
  optional boolean bool_col;
  optional int32 tinyint_col;
  optional int32 smallint_col;
  optional int32 int_col;
  optional int64 bigint_col;
  optional float float_col;
  optional double double_col;
  optional binary date_string_col;
  optional binary string_col;
  optional int96 timestamp_col;
}
```
```
id:              OPTIONAL INT32 R:0 D:1
bool_col:        OPTIONAL BOOLEAN R:0 D:1
tinyint_col:     OPTIONAL INT32 R:0 D:1
smallint_col:    OPTIONAL INT32 R:0 D:1
int_col:         OPTIONAL INT32 R:0 D:1
bigint_col:      OPTIONAL INT64 R:0 D:1
float_col:       OPTIONAL FLOAT R:0 D:1
double_col:      OPTIONAL DOUBLE R:0 D:1
date_string_col: OPTIONAL BINARY R:0 D:1
string_col:      OPTIONAL BINARY R:0 D:1
timestamp_col:   OPTIONAL INT96 R:0 D:1
```

