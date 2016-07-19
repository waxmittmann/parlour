//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.parlour

import scala.util.Failure

import java.util.UUID

import com.twitter.scalding.Csv

import scalikejdbc.{AutoSession, ConnectionPool, SQL}

import org.specs2.execute.{AsResult, Result}
import org.specs2.specification.Fixture

import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

class ImportSqoopSpec extends ThermometerSpec { def is = s2"""
  Import Sqoop Flow/Job/Execution Spec
  ==========================

  end to end console job test         $endToEndConsole
  end to end sqoop execution test     $endToEndExecution
  sqoop execution test w/ no sink     $endToEndExecutionNoSink
  end to end sqoop with SELECT        $endToEndWithQuery
  end to end sqoop with drop delim    $endToEndExecutionWithDropImportDelim
  end to end sqoop with replace delim $endToEndExecutionWithReplaceImportDelim
  failing sqoop execution fails       $failingExecution
"""

  def endToEndConsole = withData(List(
    ("003", "Robin Hood", 0, 0),
    ("004", "Little John", -1, -100)
  ))( dsl => {
    val dst  = dir </> "output"
    val args = Map[String, String](
      "connection-string"    -> dsl.getConnectionString.get,
      "username"             -> dsl.getUsername.get,
      "password"             -> dsl.getPassword.get,
      "table-name"           -> dsl.getTableName.get,
      "target-dir"           -> dst.toString,
      "mappers"              -> "1",
      "fields-terminated-by" -> ",",
      "hadoop-mapred-home"   -> dsl.getHadoopMapRedHome.get
    )

    executesOk(ImportSqoopConsoleJob.job, args.mapValues(List(_)))
    facts(dst </> "part-m-00000" ==> lines(List(
      "003,Robin Hood,0,0",
      "004,Little John,-1,-100"
    )))
  })

  def endToEndExecution = withData(List(
    ("abc", "Batman", 100000, 10000000)
  ))( dsl => {
    val sink      = Csv((dir </> "output").toString)
    val execution = SqoopExecution.sqoopImport(dsl, sink)
    executesOk(execution)
    facts(dir </> "output" </> "part-m-00000" ==> lines(List(
      "abc,Batman,100000,10000000"
    )))
  })

  def endToEndExecutionNoSink = withData(List(
    ("abc", "Batman", 100000, 10000000)
  ))( dsl => {
    val withDelimiter = dsl fieldsTerminatedBy('|')
    val withTargetDir = withDelimiter targetDir((dir </> "output").toString)

    val execution = SqoopExecution.sqoopImport(withTargetDir)
    executesOk(execution)
    facts(dir </> "output" </> "part-m-00000" ==> lines(List(
      "abc|Batman|100000|10000000"
    )))
  })

  def endToEndWithQuery = withData(List(
    ("001", "Micky Mouse",  10,  1000),
    ("002", "Donald Duck", 100, 10000)
  ))( dsl => {
    //In this test we need to know what's the name of the table created automatically in `withData`; luckily it was set on dsl.tableName; we use this name in SELECT query.
    //Then we set dsl.tableName=null, to tell Sqoop to use provided Sql query to fetch data on import.
    val tableName     = dsl.getTableName.get
    val withQuery     = dsl.tableName(null).sqlQuery(s"SELECT id, customer, balance FROM $tableName WHERE balance > 10 AND " + "$CONDITIONS")
    val withTargetDir = withQuery.targetDir((dir </> "output").toString)

    executesOk(SqoopExecution.sqoopImport(withTargetDir))
    facts(dir </> "output" </> "part-m-00000" ==> lines(List(
      "002,Donald Duck,100"
    )))
  })

  def failingExecution = withData(List())( dsl => {
    val withTableName = dsl tableName("INVALID")

    val sink      = Csv((dir </> "output").toString)
    val execution = SqoopExecution.sqoopImport(withTableName, sink)
    execute(execution) must beLike { case Failure(_) => ok }
  })

  def endToEndExecutionWithDropImportDelim = withData(List(
    ("abc", "Batman\n", 100000, 10000000)
  ))( dsl => {
    val withDelimiter       = dsl fieldsTerminatedBy('|')
    val withDropImportDelim = withDelimiter targetDir((dir </> "output").toString) hiveDropImportDelims

    val execution = SqoopExecution.sqoopImport(withDropImportDelim)
    executesOk(execution)
    facts(dir </> "output" </> "part-m-00000" ==> lines(List(
      "abc|Batman|100000|10000000"
    )))
  })

  def endToEndExecutionWithReplaceImportDelim = withData(List(
    ("abc", "Batman\n", 100000, 10000000)
  ))( dsl => {
    val withDelimiter       = dsl fieldsTerminatedBy('|')
    val withReplaceImportDelim = withDelimiter targetDir((dir </> "output").toString) hiveImportDelimsReplacement("newLine")

    val execution = SqoopExecution.sqoopImport(withReplaceImportDelim)
    executesOk(execution)
    facts(dir </> "output" </> "part-m-00000" ==> lines(List(
      "abc|BatmannewLine|100000|10000000"
    )))
  })
}

/** Provides a temporary dummy table for sqoop iport tests */
case class withData(data: List[(String, String, Int, Int)]) extends Fixture[ParlourImportDsl] {
  Class.forName("org.hsqldb.jdbcDriver")

  def apply[R: AsResult](test: ParlourImportDsl => R): Result = {
    val table   = s"table_${UUID.randomUUID.toString.replace('-', '_')}"
    val connStr = "jdbc:hsqldb:mem:sqoopdb"
    val user    = "sa"
    val pwd     = ""

    ConnectionPool.singleton(connStr, user, pwd)
    implicit val session = AutoSession

    SQL(s"""
      create table $table (
        id varchar(20),
        customer varchar(20),
        balance integer,
        balance_cents integer
      )
    """).execute.apply()

    val inserted = data.map { case (id, customer, balance, balance_cents) =>
      SQL(s"""
        insert into $table(id, customer, balance, balance_cents)
        values (?, ?, ?, ?)
      """).bind(id, customer, balance, balance_cents).update.apply()
    }

    if (inserted.exists(_ != 1)){
      failure.updateMessage("failed to insert data into the test table")
    }
    else {
      val dsl = new ParlourImportDsl()
      .connectionString(connStr)
      .username(user)
      .password(pwd)
      .tableName(table)
      .numberOfMappers(1)
      .hadoopMapRedHome(System.getProperty("user.home") + "/.ivy2/cache")

      AsResult(test(dsl))
    }
  }
}
