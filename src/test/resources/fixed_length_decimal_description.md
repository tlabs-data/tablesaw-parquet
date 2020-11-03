From https://github.com/apache/parquet-testing

```
message spark_schema {
  optional fixed_len_byte_array(11) value (DECIMAL(25,2));
}
```

```
file schema: spark_schema 
--------------------------------------------------------------------------------
value:       OPTIONAL FIXED_LEN_BYTE_ARRAY O:DECIMAL R:0 D:1

row group 1: RC:24 TS:319 OFFSET:4 
--------------------------------------------------------------------------------
value:        FIXED_LEN_BYTE_ARRAY UNCOMPRESSED DO:0 FPO:4 SZ:319/319/1,00 VC:24 ENC:BIT_PACKED,RLE,PLAIN ST:[no stats for this column]
```
