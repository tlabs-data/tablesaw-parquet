Tablesaw parquet I/O
=======

[![Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://github.com/tlabs-data/tablesaw-parquet/actions/workflows/main.yml/badge.svg)](https://github.com/tlabs-data/tablesaw-parquet/actions)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=coverage)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)

## Overview

__tablesaw-parquet__  is an [Apache Parquet](https://parquet.apache.org/) reader and writer for the [Tablesaw](https://github.com/jtablesaw/tablesaw) project implemented using [parquet-mr](https://github.com/apache/parquet-mr).

## Versioning

__tablesaw__  and  __tablesaw-parquet__  have different release schedules and versioning schemes. New  __tablesaw-parquet__  features will be available for the most recent *and* for the previous released version of  __tablesaw__ . Older tablesaw releases will not get updates.

The first supported  __tablesaw__  version is  __v0.38.2__ .

__tablesaw-parquet__  follows the [semantic versioning](https://semver.org/) scheme.

## Getting started

#### Adding the tablesaw-parquet package to your project

Add tablesaw-core and tablesaw-parquet to your project as follows:

__maven:__

```xml
<properties>
    <tablesaw.version>0.42.0</tablesaw.version>
    <tablesaw-parquet.version>0.10.0</tablesaw-parquet.version>
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

```groovy
ext {
    tablesawVersion = "0.42.0"
    tablesawParquetVersion = "0.10.0"
}

dependencies {
    implementation("tech.tablesaw:tablesaw-core:${tablesawVersion}")
    implementation("net.tlabs-data:tablesaw_${tablesawVersion}-parquet:${tablesawParquetVersion}")
}
```

#### Reading and writing local parquet files

Read and write your parquet files using the following patterns:

```java
// using the file name
Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(FILENAME).build());
new TablesawParquetWriter().write(table, TablesawParquetWriteOptions.builder(FILENAME).build());
// using a File object
Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(FILE).build());
new TablesawParquetWriter().write(table, TablesawParquetWriteOptions.builder(FILE).build());
```

Using a  __tablesaw__  Source object is also supported for reading parquet files using default options:

```java
Table table = new TablesawParquetReader().read(new Source(FILE));
```

Alternatively, you can manually register the parquet reader and writer to use  __tablesaw__  classic patterns:

```java
TablesawParquet.register();

Table table = Table.read().file(FILENAME);
table = Table.read().file(new File(FILENAME));
table = Table.read().usingOptions(TablesawParquetReadOptions.builder(FILENAME));

table.write().usingOptions(TablesawParquetWriteOptions.builder(FILENAME).build());
```

The file extension must be  __".parquet"__  when using `Table.read().file()`.

__Note that all write methods not mentioned above are not supported and will throw a RuntimeException.__

__As parquet is a binary format, reading from character-based input is not supported.__

#### Reading remote parquet files

Reading remote parquet files is supported starting from `v0.10.0`. See [Working with remote files](WORKING_WITH_REMOTE_FILES.md) for tested examples and implementation details.

##### Reading from hadoop-supported file systems:

Parquet files on [hadoop-supported file systems](https://hadoop.apache.org/docs/r3.2.3/) can be read using:

```java
Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(URI).build());
```

##### Reading from http(s)/ftp(s):

Parquet files from http or ftp servers can be read using:

```java
Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(URL).build());
```

You can also register the parquet reader and use the  __tablesaw__  read method. For this to work, the URL  __must__  end with  __".parquet"__  as there is currently no [parquet MIME type](https://issues.apache.org/jira/browse/PARQUET-1889):

```java
TablesawParquet.register();

Table.read().url(URL);
```

##### Reading from user-provided InputStream:

Reading from a user-provided InpustStream is provided as a courtesy and only possible by using:

```java
Table table = new TablesawParquetReader().read(new Source(INPUTSTREAM));
```

## Data type conversion

#### Default conversion when reading a parquet file (parquet to tablesaw)

Non-annotated [parquet data types](https://github.com/apache/parquet-format) are converted to  __tablesaw__  column types as follows:

| Parquet type | Tablesaw column type | Notes |
|--------------|----------------------|-------|
| BOOLEAN  | BooleanColumn |  |
| INT32 | IntColumn |  |
| INT64 | LongColumn |  |
| INT96 | StringColumn (default) or InstantColumn | Managed by the *convertInt96ToTimestamp* option |
| FLOAT | DoubleColumn (default) or FloatColumn | Managed by the *minimizeColumnSizes* option |
| DOUBLE | DoubleColumn |  |
| BYTE_ARRAY | StringColumn | Managed by the *withUnnanotatedBinaryAs* option |

Annotated [parquet logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) are converted to  __tablesaw__  column types as follows:

| Parquet logical type | Tablesaw column type | Notes |
|----------------------|----------------------|-------|
| STRING  | StringColumn |  |
| ENUM | StringColumn |  |
| UUID | StringColumn | *Not formatted as a UUID.*  __Not tested__  |
| Signed Integers | ShortColumn or IntColumn or LongColumn | ShortColumn only with the *minimizeColumnSizes* option |
| Unsigned Integers  | ShortColumn or IntColumn or LongColumn | ShortColumn only with the *minimizeColumnSizes* option |
| DECIMAL | DoubleColumn |  |
| DATE | DateColumn |  |
| TIME | TimeColumn |  |
| TIMESTAMP | DateTimeColumn or InstantColumn | Timestamps normalized to UTC are converted to Instant, others as LocalDateTime |
| INTERVAL | StringColumn | ISO String representation of the interval.  __Not tested__  |
| JSON | TextColumn |  __Not tested__  |
| BSON |  __Not read__  |  __Not tested__  |
| Nested Types | TextColumn | Textual representation of the nested type, see *withManageGroupsAs* option |

Parquet also supports repeated fields (multiple values for the same field); we handle these as the Nested Types: by default a string representation of the repeated fields is stored in a TextColumn. The same *withManageGroupsAs* option is used to change this behavior.

Due to the lack of parquet files for testing, some logical type conversion are currently not tested (INTERVAL, UUID, JSON, BSON).

Keep in mind that all tablesaw columns storing time (TimeColumn, DateTimeColumn and InstantColumn) use MILLIS precision, if read from a parquet file with better time precision (MICROS or NANOS) the values will be truncated (in the current tablesaw implementation).

#### Using `columnTypes` and `columnTypesPartial` options to control conversion

Starting from `v0.9.0`, tablesaw-parquet allows a better control of data conversion and column filtering when reading a parquet file: using one of `TablesawParquetReadOptions` several `columnTypes` and `columnTypesPartial` methods, users can assign compatible tablesaw column types to all or some of the parquet file columns.

Only a limited set of possible conversions is implemented:
* Widening primitive conversions for integers (e.g. INT32 column can be read as a LongColumn)
* Widening primitive conversions for floats (e.g. FLOAT32 column can be read as a DoubleColumn)
* Switching between TextColumn and StringColumn for any textual columns
* Switching between InstantColumn and DateTimeColumn for timestamps - in this case the UTC annotation is ignored

Other conversions (e.g. converting numbers or dates to StringColumn) are not implemented but can be done using one of the several  __tablesaw__  features for such operations.

Note that the `ColumnType.SKIP` column type can be used with these options to filter out any column from the parquet file.

#### Conversion done when writing a parquet file (tablesaw to parquet)

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
| TimeColumn | INT32 (TIME: MILLIS, not UTC) | *Changed in v0.11.0, was INT64 (TIME: NANOS, not UTC) before* |
| DateColumn | INT32 (DATE) |  |
| DateTimeColumn | INT64 (TIMESTAMP: MILLIS, not UTC) |  |
| InstantColumn | INT64 (TIMESTAMP: MILLIS, UTC) |  |

Note that a tablesaw Table written to parquet and read back with default conversion will have the following changes:

* TextColumns will be read back as StringColumns.
* If the *minimizeColumnSizes* option is not set (which is the default), Floats will be changed to Doubles and Shorts to Integers.

## Features

#### Column filtering

Starting from `v0.8.0`, column filtering is possible using the `TablesawParquetReadOptions.withOnlyTheseColumns` method.

The resulting Table will only contain the requested columns, in the order they were specified.

Starting from `v0.9.0`, column filtering is also possible using `ColumnType.SKIP` as a column type with one of `TablesawParquetReadOptions` several `columnTypes` and `columnTypesPartial` methods. 

Filtering is done by schema projection in the underlying reader: columns that are not required are not even read from the file. This is highly efficient when reading only a few columns, even from very large parquet files.

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

## Compatibility testing

Testing the compatibility with other sources of parquet files is paramount. We currently use two sets of test files for that:

* Test files from the [parquet-testing](https://github.com/apache/parquet-testing) project
* Test files generated from [pandas](https://pandas.pydata.org/) dataframe with both [fastparquet](https://fastparquet.readthedocs.io/en/latest/) and [pyArrow](https://pyarrow.readthedocs.io/en/latest/)

Note that we currently do not run the tests on Windows (see this [github issue](https://github.com/tlabs-data/tablesaw-parquet/issues/3) for details).

## How To Contribute

Users are welcome to contribute to this project.

Bugs or problems with the library should be reported on [github](https://github.com/tlabs-data/tablesaw-parquet/issues). If you report an issue you have when reading a parquet file, please attach a sample test file, so we can reproduce and correct the issue. Feature requests are also welcome, but there is no guarantee we will be able to implement them quickly...

Users are also welcome to contribute (small) test parquet files from different sources or with different column types than the ones currently tested. In this case please provide a quick description of the file content and origin. If you are able to provide a pull request with the tests implemented all the better.

Users are encouraged to report successful (or unsuccessful...) use of the library with remote file systems to complement the current use cases documented.

We also accept pull requests with code or documentation improvements. We prefer pull requests linked to an existing issue, create one if needed. Code quality is important, code efficiency is even more important, and testing is mandatory.

## Code of conduct

We do not yet have an official code of conduct. In the meantime, be nice to the developers and to each others.
