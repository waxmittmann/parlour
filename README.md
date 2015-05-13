Parlour
=======

[![Build Status](https://travis-ci.org/CommBank/parlour.svg?branch=master)](https://travis-ci.org/CommBank/parlour)
[![Gitter chat](https://badges.gitter.im/CommBank.png)](https://gitter.im/CommBank)

> ***Parlour.*** *a place that sells scoops of ice-cream; a cascading-sqoop integration.*

Parlour provides a basic Cascading/Scalding Sqoop integration allowing import to and export from HDFS.

It also provides support for the Cloudera/Teradata Connector.

[Scaladoc](https://commbank.github.io/parlour/latest/api/index.html)

[Github Pages](http://commbank.github.io/parlour/)

Third-Party Libraries
---------------------

In order to get Parlour to work - you will need to include the following third-party JARs with the application that you use it in.

If you want to use them within the `parlour` repository - you will need to put them in `lib/`.

**Oracle Support**:

- `ojdbc6.jar`: the Oracle JDBC Adapter

**Teradata Support**:

- `sqoop-connector-teradata-1.2c5.jar`: Cloudera Connector Powered by Teradata
- `tdgssconfig.jar`: Teradata Driver (Security configuration)
- `terajdbc4.jar`: Teradata JDBC Adapter


Cascade Job
-----------

    import au.com.cba.omnia.parlour.SqoopSyntax._

    new ImportSqoopJob(
      TeradataParlourImportDsl()
        .inputMethod(SplitByAmp)
        .connectString("jdbc:teradata://some.server/database=DB1")
        .username("some username")
        .password(System.getenv("DATABASE_PASSWORD"))
        .tableName("some table").toSqoopOptions,
      TypedTsv[String]("hdfs/path/to/target/dir")
    )(args)

    new ExportSqoopJob(
      TeradataParlourExportDsl()
       .outputMethod(BatchInsert)
       .connectionString("jdbc:teradata://some.server/database=DB1")
       .username("some username")
       .password(System.getenv("DATABASE_PASSWORD"))
       .tableName("some table").toSqoopOptions,
      TypedPsv[String]("hdfs/path/to/data/to/export")
    )(args)


Console Job
-----------

Parlour includes a sample job that can be invoked from the command-line:

    hadoop jar <parlour-jar> \
        com.twitter.scalding.Tool \
        au.com.cba.omnia.parlour.ExportSqoopConsoleJob \
        --hdfs \
        --export-dir /data/on/hdfs/to/sqoop \
        --teradata-method internal.fastload \
        --teradata-fastload-socket-hostname myhostname1 \
        --connection-string "jdbc:teradata://database/database=test" \
        --table-name test \
        --username user1 \
        --password $PASSWORD \
        --mappers 1 \
        --input-field-delimiter \| \
        --input-line-delimiter \n


Teradata Fastload Support
-------------------------

Teradata Internal Fastload requires the use of a coordinating service that runs on the machine that launches the jobs.

As a result - you may need to manually specify which  adapter the service should be bound to.
This is done using:
 
    TeradataParlourExportDsl(sqoopOptions).fastloadSocketHostName("myhostname")

