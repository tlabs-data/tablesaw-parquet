Note: timestamp exported by default in us and adjustedToUTC=true

```
message schema {
  optional boolean boolean;
  optional int32 ubyte (UINT_8);
  optional int32 byte (INT_8);
  optional int32 ushort (UINT_16);
  optional int32 short (INT_16);
  optional int32 uint (UINT_32);
  optional int32 int;
  optional int64 ulong (UINT_64);
  optional int64 long;
  optional float float;
  optional double double;
  optional int64 datetime (TIMESTAMP_MICROS);
  optional binary string (UTF8);
}
```
```
row group 1: RC:2 TS:534 OFFSET:4 
--------------------------------------------------------------------------------
boolean:      BOOLEAN GZIP DO:0 FPO:4 SZ:46/32/0,70 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[min: false, max: true, num_nulls: 0]
ubyte:        INT32 GZIP DO:0 FPO:103 SZ:49/39/0,80 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[no stats for this column]
byte:         INT32 GZIP DO:0 FPO:211 SZ:51/39/0,76 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[min: -127, max: 1, num_nulls: 0]
ushort:       INT32 GZIP DO:0 FPO:320 SZ:49/39/0,80 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[no stats for this column]
short:        INT32 GZIP DO:0 FPO:429 SZ:51/39/0,76 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[min: -32768, max: 0, num_nulls: 0]
uint:         INT32 GZIP DO:0 FPO:539 SZ:50/39/0,78 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[no stats for this column]
int:          INT32 GZIP DO:0 FPO:647 SZ:52/39/0,75 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[min: -65000, max: 0, num_nulls: 0]
ulong:        INT64 GZIP DO:0 FPO:756 SZ:51/47/0,92 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[no stats for this column]
long:         INT64 GZIP DO:0 FPO:874 SZ:53/47/0,89 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[min: -1000000000, max: 0, num_nulls: 0]
float:        FLOAT GZIP DO:0 FPO:993 SZ:49/35/0,71 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[num_nulls: 1, min/max not defined]
double:       DOUBLE GZIP DO:0 FPO:1089 SZ:45/39/0,87 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[num_nulls: 1, min/max not defined]
datetime:     INT64 GZIP DO:0 FPO:1182 SZ:57/47/0,82 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[min: 2021-04-23T00:00:00.000000, max: 2021-04-23T00:00:01.000000, num_nulls: 0]
string:       BINARY GZIP DO:0 FPO:1309 SZ:58/53/0,91 VC:2 ENC:BIT_PACKED,PLAIN,RLE ST:[no stats for this column]
```

