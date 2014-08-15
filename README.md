Parlour
=======

[![Build Status](https://travis-ci.org/CommBank/parlour.svg?branch=master)](https://travis-ci.org/CommBank/parlour)
[![Gitter chat](https://badges.gitter.im/CommBank.png)](https://gitter.im/CommBank)

> ***Parlour.*** *a place that sells scoops of ice-cream; a cascading-sqoop integration.*

Parlour provides a basic Cascading/Scalding Sqoop integration allowing export from HDFS.

It also provides for support for the Cloudera/Teradata Connector.

Cascade Job
-----------

    import au.com.cba.omnia.parlour.SqoopSyntax._

    new ExportSqoopJob(
      sqoopOptions()
       .teradata(BatchInsert)
       .connectionString("jdbc:teradata://some.server/database=DB1")
       .username("some username")
       .password(System.getenv("DATABASE_PASSWORD"))
       .tableName("some table"),
      TypedPsv[String]("hdfs/path/to/data/to/export")
    )(args)


Console Job
-----------

Parlour includes a sample job that can be invoked from the command-line:

    hadoop jar <parlour-jar> \
        com.twitter.scalding.Tool \
        au.com.cba.omnia.parlour.ExportSqoopConsoleJob \
        --hdfs \
        --input /data/on/hdfs/to/sqoop \
        --teradata \
        --teradata-method internal.fastload \
        --teradata-internal-fastload-host-adapter myhostname1 \
        --connection-string "jdbc:teradata://database/database=test" \
        --table-name test \
        --username user1 \
        --password $PASSWORD \
        --mappers 1 \
        --input-field-delimiter \| \
        --input-line-delimiter \n


Teradata Fastload Support
-------------------------

Teradata Internal Fastload requires the use of a coordinating service that runs on the
machine that launches the jobs.

As a result - you may need to manually specify which  adapter the service should be bound to.
 This is done using `sqoopOptions.teradata(InternalFastload, Some("myhostname"))`.

