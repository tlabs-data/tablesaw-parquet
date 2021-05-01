Note: timestamp coerced to ms, exported by default with adjustedToUTC=false

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
  optional int64 datetime (TIMESTAMP_MILLIS);
  optional binary string (UTF8);
}
```
```
row group 1: RC:2 TS:1450 OFFSET:4 
--------------------------------------------------------------------------------
boolean:      BOOLEAN GZIP DO:0 FPO:4 SZ:60/40/0,67 VC:2 ENC:PLAIN,RLE ST:[min: false, max: true, num_nulls: 0]
ubyte:        INT32 GZIP DO:121 FPO:161 SZ:102/64/0,63 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: 0, max: 127, num_nulls: 0]
byte:         INT32 GZIP DO:292 FPO:334 SZ:116/76/0,66 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: -127, max: 1, num_nulls: 0]
ushort:       INT32 GZIP DO:488 FPO:528 SZ:102/64/0,63 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: 0, max: 32768, num_nulls: 0]
short:        INT32 GZIP DO:660 FPO:700 SZ:114/76/0,67 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: -32768, max: 0, num_nulls: 0]
uint:         INT32 GZIP DO:855 FPO:897 SZ:104/64/0,62 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: 0, max: 65000, num_nulls: 0]
int:          INT32 GZIP DO:1027 FPO:1069 SZ:116/76/0,66 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: -65000, max: 0, num_nulls: 0]
ulong:        INT64 GZIP DO:1222 FPO:1264 SZ:112/80/0,71 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: 0, max: 1000000000, num_nulls: 0]
long:         INT64 GZIP DO:1411 FPO:1454 SZ:133/100/0,75 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: -1000000000, max: 0, num_nulls: 0]
float:        FLOAT GZIP DO:1640 FPO:1678 SZ:112/72/0,64 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: 1.0, max: 1.0, num_nulls: 1]
double:       DOUBLE GZIP DO:1833 FPO:1870 SZ:127/92/0,72 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: -0.0, max: 0.0, num_nulls: 1]
datetime:     INT64 GZIP DO:2058 FPO:2104 SZ:136/100/0,74 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: 2021-04-23T00:00:00.000, max: 2021-04-23T00:00:01.000, num_nulls: 0]
string:       BINARY GZIP DO:2294 FPO:2342 SZ:116/84/0,72 VC:2 ENC:PLAIN,RLE,RLE_DICTIONARY ST:[min: string1, max: string2, num_nulls: 0]
```

