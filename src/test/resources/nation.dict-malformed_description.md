From https://github.com/apache/parquet-testing

```
message m {
  optional int32 nation_key;
  optional binary name;
  optional int32 region_key;
  optional binary comment_col;
}
```

```
file schema: m 
--------------------------------------------------------------------------------
nation_key:  OPTIONAL INT32 R:0 D:1
name:        OPTIONAL BINARY R:0 D:1
region_key:  OPTIONAL INT32 R:0 D:1
comment_col: OPTIONAL BINARY R:0 D:1

row group 1: RC:25 TS:0 OFFSET:4 
--------------------------------------------------------------------------------
nation_key:   INT32 UNCOMPRESSED DO:0 FPO:4 SZ:125/125/1,00 VC:25 ST:[no stats for this column]
name:         BINARY UNCOMPRESSED DO:0 FPO:129 SZ:322/322/1,00 VC:25 ST:[no stats for this column]
region_key:   INT32 UNCOMPRESSED DO:0 FPO:466 SZ:125/125/1,00 VC:25 ST:[no stats for this column]
comment_col:  BINARY UNCOMPRESSED DO:0 FPO:591 SZ:2002/2002/1,00 VC:25 ST:[no stats for this column]
```
