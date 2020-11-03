From https://github.com/apache/parquet-testing

```
message spark_schema {
  optional binary a (UTF8);
  required int32 b;
  required double c;
  required boolean d;
  optional group e (LIST) {
    repeated group list {
      required int32 element;
    }
  }
}
```

```
a:           OPTIONAL BINARY O:UTF8 R:0 D:1
b:           REQUIRED INT32 R:0 D:0
c:           REQUIRED DOUBLE R:0 D:0
d:           REQUIRED BOOLEAN R:0 D:0
e:           OPTIONAL F:1 
.list:       REPEATED F:1 
..element:   REQUIRED INT32 R:1 D:2
```