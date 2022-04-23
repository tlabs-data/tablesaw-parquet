Using tablesaw-parquet with remote parquet files
=======

## Reading from remote parquet files

Reading remote parquet files is supported starting from `v0.10.0`.

__When using the URI or URL based builder, a sanitized URI (with no credentials and no query parts) is used by default for the table name.__

#### hadoop-supported file systems

Reading parquet files from an hadoop-supported file system is possible by adding the hadoop specific module in your classpath 
and using the hadoop specific URI.

See the [hadoop documentation](https://hadoop.apache.org/docs/r3.2.3/) for details.

##### What was tested:

__Reading from an AWS s3 bucket:__

Add the hadoop-aws module in your dependencies:

```xml
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aws</artifactId>
    <version>3.2.3</version>
  </dependency>
```

You  __must__  use the same hadoop version than tablesaw-parquet.

Read a parquet file from an AWS s3 bucket (using file-based credentials):

```java
String PARQUET_FILE_URI = "s3a://<bucket-name>/<file-key>";
Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(PARQUET_FILE_URI).build());
```

#### http(s) and ftp(s) accessible parquet file

Reading a parquet file from an http or ftp server is possible without additional dependencies. 
The current implementation downloads the whole file on the local file system before reading it.

URLs or URIs can be used:

```java
String PARQUET_FILE_URL = "http://<server>/<file-path>";
Table table = new TablesawParquetReader().read(TablesawParquetReadOptions.builder(PARQUET_FILE_URL).build());
```

##### What was tested:

__Unit test with local http and ftp server:__

Unit tests for reading from http and ftp servers are implemented in the project. Using ftp credentials in URL is also covered.

__Manual test with AWS (https) pre-signed URL:__

A manual test using a pre-signed AWS URL was successfully performed.

#### Reading from a user-provided InputStream

Reading from a user-provided InpustStream is provided as a courtesy and only possible by using:

```java
Table table = new TablesawParquetReader().read(new Source(INPUTSTREAM));
```

The current implementation downloads the whole stream on the local file system before reading it.

__When reading from a stream, the resulting table has an empty name by default.__

##### What was tested:

Unit test using a Source from FileInputStream included.

Unit tests for reading from http and ftp use the same internal code.
