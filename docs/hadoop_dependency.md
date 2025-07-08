Dependency on hadoop libraries
=======

**You don't need to install hadoop to use this library.**

We are using the parquet reader from [parquet-hadoop](https://github.com/apache/parquet-mr/tree/master/parquet-hadoop) to read
parquet files. This package depends on the `hadoop-common` and `hadoop-mapreduce-client-core` Java libraries with a `provided` scope - this is why these libraries are included here.

Efforts to avoid depending on hadoop libraries can be followed on this [open issue on the parquet-mr JIRA](https://issues.apache.org/jira/browse/PARQUET-1126).

## Exclude transitive dependencies from hadoop libraries

The hadoop libraries come with many transitive dependencies. Since `v0.13.0` those listed below are excluded from `tablesaw-parquet`.

For previous versions, you will need to exclude them on your side as shown below:

The following transitive dependencies exclusions were successfully tested with all versions since `v0.10.0`:

```xml
    <dependency>
      <groupId>tech.tablesaw</groupId>
      <artifactId>tablesaw-core</artifactId>
      <version>0.43.1</version>
    </dependency>
    <dependency>
      <groupId>net.tlabs-data</groupId>
      <artifactId>tablesaw_0.43.1-parquet</artifactId>
      <version>0.10.0</version>
      <exclusions>
        <exclusion>
          <groupId>org.eclipse.jetty</groupId>
          <artifactId>*</artifactId>
        </exclusion>
        <exclusion>
          <groupId>javax.servlet.jsp</groupId>
          <artifactId>*</artifactId>
        </exclusion>
        <exclusion>
          <groupId>com.sun.jersey</groupId>
          <artifactId>*</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.apache.curator</groupId>
          <artifactId>*</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.apache.zookeeper</groupId>
          <artifactId>*</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.apache.kerby</groupId>
          <artifactId>*</artifactId>
        </exclusion>
        <exclusion>
          <groupId>com.google.protobuf</groupId>
          <artifactId>*</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
```

## Using this library on Windows

Reading parquet files on Windows requires no additional installation.

Writing parquet files on Windows requires the presence of `winutils.exe` in the `%HADOOP_HOME%/bin` folder. This tool can be downloaded from [this repository](https://github.com/cdarlint/winutils).

An exact version match does no seem required: the current unit tests on Windows use winutils version 3.2.2 while the project uses hadoop 3.2.3.
