Tablesaw parquet I/O
=======

[![Apache 2.0](https://img.shields.io/github/license/nebula-plugins/nebula-project-plugin.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://github.com/tlabs-data/tablesaw-parquet/actions/workflows/main.yml/badge.svg)](https://github.com/tlabs-data/tablesaw-parquet/actions)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=coverage)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)

## Overview

__tablesaw-parquet__  is a [parquet](https://parquet.apache.org/) reader and writer for the [Tablesaw](https://github.com/jtablesaw/tablesaw) project implemented using [parquet-mr](https://github.com/apache/parquet-mr).

## Versioning

__tablesaw__  and  __tablesaw-parquet__  have different release schedules and versioning schemes. New  __tablesaw-parquet__  features will be available for the most recent *and* for the previous released version of  __tablesaw__ . Older tablesaw releases will not get updates.

The first supported  __tablesaw__  version is  __v0.38.2__ .

__tablesaw-parquet__  follows the [semantic versioning](https://semver.org/) scheme.

## Getting started

Add tablesaw-core and tablesaw-parquet to your project as follows:

__maven:__

```xml
<properties>
    <tablesaw.version>TABLESAW_VERSION</tablesaw.version>
    <tablesaw-parquet.version>TABLESAW-PARQUET_VERSION</tablesaw-parquet.version>
</properties>
<dependencies>
  <dependency>
    <groupId>tech.tablesaw</groupId>
    <artifactId>tablesaw-core</artifactId>
    <version>${tablesaw.version}</version>
  </dependency>
  <dependency>
    <groupId>net.tlabs-data</groupId>
    <artifactId>tablesaw_${tablesaw.version}-parquet</artifactId>
    <version>${tablesaw-parquet.version}</version>
  </dependency>
<dependencies>
```

__gradle:__

```
TODO
```

Read your parquet file as a  __tablesaw__  Table using the following idiom:

```java
Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(FILENAME).build());
```

## Data type conversion

#### When reading a parquet file (parquet to tablesaw)

Non-annotated [parquet data types](https://github.com/apache/parquet-format) are converted to  __tablesaw__  column types as follow:

| Parquet type | Tablesaw column type | Notes |
|--------------|----------------------|-------|
| BOOLEAN  | BooleanColumn |  |
| INT32 | IntColumn |  |
| INT64 | LongColumn |  |
| INT96 | StringColumn (default) or InstantColumn | Managed by the *convertInt96ToTimestamp* option |
| FLOAT | DoubleColumn (default) or FloatColumn | Managed by the *minimizeColumnSizes* option |
| DOUBLE | DoubleColumn |  |
| BYTE_ARRAY | StringColumn | Managed by the *withUnnanotatedBinaryAs* option |

Annotated [parquet logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) are converted to  __tablesaw__  column types as follow:

| Parquet logical type | Tablesaw column type | Notes |
|----------------------|----------------------|-------|
| STRING  | StringColumn |  |
| ENUM | StringColumn |  |
| UUID | StringColumn | *Not formatted as a UUID*  __Not tested__  |
| Signed Integers | ShortColumn or IntColumn or LongColumn | ShortColumn only with the *minimizeColumnSizes* option |
| Unsigned Integers  | ShortColumn or IntColumn or LongColumn | ShortColumn only with the *minimizeColumnSizes* option |
| DECIMAL | DoubleColumn |  |
| DATE | DateColumn |  |
| TIME | TimeColumn |  |
| TIMESTAMP | DateTimeColumn or InstantColumn | Timestamps normalized to UTC are converted to Instant, others as LocalDateTime |
| INTERVAL | StringColumn | ISO String representation of the interval  __Not tested__  |
| JSON | TextColumn |  |
| BSON |  __Not read__  |  __Not tested__  |
| Nested Types | TextColumn | Textual representation of the nested type, see *withManageGroupsAs* option |

Parquet also handles repeated fields (multiple values for the same field), we handle these as the Nested Types: by default a string representation of the repeated fields is stored in a TextColumn. The same *withManageGroupsAs* option is used to change this behavior.

Due to the lack of parquet files for testing, some logical type conversion are currently not tested (INTERVAL, UUID, BSON).

Keep in mind that all tablesaw columns storing time (TimeColumn, DateTimeColumn and InstantColumn) use MILLIS precision, if read from a parquet file with better time precision (MICROS or NANOS) the values will be truncated (in the current tablesaw implementation).

#### When writing a parquet file (tablesaw to parquet)

| Tablesaw column type | Parquet type (logical type) | Notes |
|----------------------|-----------------------------|-------|
| BooleanColumn | BOOLEAN |  |
| ShortColumn | INT32 (Integer: 16 bits, signed)|  |
| IntColumn | INT32 |  |
| LongColumn | INT64 |  |
| FloatColumn | FLOAT |  |
| DoubleColumn | DOUBLE |  |
| StringColumn | BINARY (STRING) |  |
| TextColumn | BINARY (STRING) |  |
| TimeColumn | INT64 (TIME: NANOS, not UTC) | *Will likely change to INT32 MILLIS in a future release* |
| DateColumn | INT32 (DATE) |  |
| DateTimeColumn | INT64 (TIMESTAMP: MILLIS, not UTC) |  |
| InstantColumn | INT64 (TIMESTAMP: MILLIS, UTC) |  |

Note that a tablesaw Table written to parquet and read back will have the following changes:

* TextColumns will be read back as StringColumns.
* If the *minimizeColumnSizes* option is not set (which is the default), Floats will be changed to Doubles and Shorts to Integers.

## Features

#### Column encoding

Physical reading and writing of parquet files is done by [parquet-mr](https://github.com/apache/parquet-mr). The encoding and decoding is managed by this library.

#### Compression codecs

Currently supported and tested compression codecs: UNCOMPRESSED (None), SNAPPY, GZIP, and ZSTD.

Other compression codecs *might* work when reading parquet files depending on your setup but there is no guarantee.

#### Predicate pushdown

Predicate pushdown is not supported when reading parquet files.

Parquet files written with tablesaw-parquet contain the statistics needed for predicate pushdown when reading files with other parquet readers.

#### Encryption

[Parquet Modular Encryption](https://github.com/apache/parquet-format/blob/encryption/Encryption.md) is not supported.

#### Column filtering

Additional options for filtering columns based on name or type will be added in the future.

## Compatibility testing

Testing the compatibility with other sources of parquet files is paramount. We currently use two sets of test files for that:

* Test files from the [parquet-testing](https://github.com/apache/parquet-testing) project
* Test files generated from [pandas](https://pandas.pydata.org/) dataframe with both [fastparquet](https://fastparquet.readthedocs.io/en/latest/) and [pyArrow](https://pyarrow.readthedocs.io/en/latest/)

Note that we currently do not run the tests on Windows (see this [github issue](https://github.com/tlabs-data/tablesaw-parquet/issues/3) for details).

## How To Contribute

Users are welcome to contribute to this project.

Bugs or problems with the library should be reported on [github](https://github.com/tlabs-data/tablesaw-parquet/issues). If you report an issue you have when reading a parquet file, please attach a sample test file so we can reproduce and correct the issue. Feature requests are also welcome, but there is no guarantee we will be able to implement them quickly...

Users are also welcome to contribute (small) test parquet files from different sources or with different column types than the ones currently tested. In this case please provide a quick description of the file content and origin. If you are able to provide a pull request with the tests implemented all the better.

We also accept pull requests with code or documentation improvements. We prefer pull requests linked to an existing issue, create one if needed. Code quality is important, code efficiency is even more important, and testing is mandatory.

## Code of conduct

We do not yet have an official code of conduct. In the meantime, be nice to each others (and to the developers).
