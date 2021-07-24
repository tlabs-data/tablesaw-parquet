Tablesaw parquet I/O
=======

[![Apache 2.0](https://img.shields.io/github/license/nebula-plugins/nebula-project-plugin.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://github.com/tlabs-data/tablesaw-parquet/actions/workflows/main.yml/badge.svg)](https://github.com/tlabs-data/tablesaw-parquet/actions)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=coverage)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=tlabs-data_tablesaw-parquet&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=tlabs-data_tablesaw-parquet)

## Overview

__tablesaw-parquet__  is a [parquet](https://github.com/apache/parquet-mr) reader and writer for the [Tablesaw](https://github.com/jtablesaw/tablesaw) project.

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



## Features

### Implemented features



### Implemented but not tested features

### Not implemented features

### Roadmap

## Contributing

