From https://github.com/apache/parquet-testing

```
message spark_schema {
  optional group b_struct {
    optional int32 b_c_int;
  }
}
```

```
file schema: spark_schema 
--------------------------------------------------------------------------------
b_struct:    OPTIONAL F:1 
.b_c_int:    OPTIONAL INT32 R:0 D:2

row group 1: RC:8 TS:27 OFFSET:4 
--------------------------------------------------------------------------------
b_struct:    
.b_c_int:     INT32 SNAPPY DO:0 FPO:4 SZ:29/27/0,93 VC:8 ENC:PLAIN,RLE,BIT_PACKED ST:[num_nulls: 8, min/max not defined]
```
