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

import java.util.UUID

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

import scala.util.Failure

import scalaz.{Failure => _, _}, Scalaz._

import cascading.flow.{Flow, FlowListener}
import cascading.tap.Tap

import com.twitter.scalding.{Args, Csv, Write}

import scalikejdbc.{AutoSession, ConnectionPool, SQL}

import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import org.specs2.execute.{AsResult, Result}
import org.specs2.specification.Fixture

class ImportSqoopSpec extends ThermometerSpec { def is = s2"""
  Import Sqoop Flow/Job/Execution Spec
  ==========================

  end to end sqoop flow test        $endToEndFlow
  end to end sqoop job test         $endToEndJob
  end to end sqoop execution test   $endToEndExecution
  sqoop execution test w/ no sink   $endToEndExecutionNoSink

  end to end sqoop flow with SELECT $endToEndFlowWithQuery

  failing sqoop job returns false   $failingJob
  sqoop job w/ exception throws     $exceptionalJob
  failing sqoop execution fails     $failingExecution
"""

  def endToEndFlow = withData(List(
    ("001", "Micky Mouse",  10,  1000),
    ("002", "Donald Duck", 100, 10000)
  ))( dsl => {
    val source = TableTap(dsl.toSqoopOptions)
    val sink   = Csv((dir </> "output").toString).createTap(Write)
    val flow   = new ImportSqoopFlow("endToEndFlow", dsl, Some(source), Some(sink))

    println(s"=========== endToEndFlow test running in $dir ===============")

    flow.complete
    flow.getFlowStats.isSuccessful must beTrue
    facts(dir </> "output" </> "part-m-00000" ==> lines(List(
      "001,Micky Mouse,10,1000",
      "002,Donald Duck,100,10000"
    )))
  })

  def endToEndJob = withData(List(
    ("003", "Robin Hood", 0, 0),
    ("004", "Little John", -1, -100)
  ))( dsl => {
    val source = TableTap(dsl.toSqoopOptions)
    val sink   = Csv((dir </> "output").toString).createTap(Write)
    val job    = new ImportSqoopJob(dsl, source, sink)(scaldingArgs)
    job.withFacts(dir </> "output" </> "part-m-00000" ==> lines(List(
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

  def endToEndFlowWithQuery = withData(List(
    ("001", "Micky Mouse",  10,  1000),
    ("002", "Donald Duck", 100, 10000)
  ))( dsl => {
    //In this test we need to know what's the name of the table created automatically in `withData`; luckily it was set on dsl.tableName; we use this name in SELECT query.
    //Then we set dsl.tableName=null, to tell Sqoop to use provided Sql query to fetch data on import.
    val tableName = dsl.getTableName.get
    val withQuery = dsl.tableName(null).sqlQuery(s"SELECT id, customer, balance FROM $tableName WHERE balance > 10 AND " + "$CONDITIONS")

    val source = TableTap(dsl.toSqoopOptions)
    val sink   = Csv((dir </> "output").toString).createTap(Write)
    val flow   = new ImportSqoopFlow("endToEndFlow", withQuery, Some(source), Some(sink))

    println(s"=========== endToEndFlow test running in $dir ===============")

    flow.complete
    flow.getFlowStats.isSuccessful must beTrue
    facts(dir </> "output" </> "part-m-00000" ==> lines(List(
      "002,Donald Duck,100"
    )))
  })

  def failingJob = withData(List())( dsl => {
    val withTableName = dsl tableName("INVALID")

    val source = TableTap(withTableName.toSqoopOptions)
    val sink   = Csv((dir </> "output").toString).createTap(Write)
    val job    = new SquishExceptionsImportSqoopJob(withTableName, source, sink)(scaldingArgs)
    (new VerifiableJob(job)).run must_== Some(s"Job failed to run <${job.name}>".left)
  })

  def exceptionalJob = withData(List())( dsl => {
    val withTableName = dsl tableName("INVALID")

    val source = TableTap(withTableName.toSqoopOptions)
    val sink   = Csv((dir </> "output").toString).createTap(Write)
    val job    = new ImportSqoopJob(withTableName, source, sink)(scaldingArgs)
    (new VerifiableJob(job)).run must beLike { case Some(\/-(_)) => ok }
  })

  def failingExecution = withData(List())( dsl => {
    val withTableName = dsl tableName("INVALID")

    val sink      = Csv((dir </> "output").toString)
    val execution = SqoopExecution.sqoopImport(withTableName, sink)
    execute(execution) must beLike { case Failure(_) => ok }
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

class SquishExceptionsImportSqoopJob(
  options: ParlourImportOptions[_],
  source: Tap[_, _, _],
  sink: Tap[_, _, _])(
  args: Args
) extends ImportSqoopJob(options, source, sink)(args) {
  override def buildFlow = {
    val flow = super.buildFlow
    flow.addListener(new SquishExceptionListener)
    flow
  }
}

class SquishExceptionListener extends FlowListener {
  def onStarting(flow: Flow[_]) {}
  def onStopping(flow: Flow[_]) {}
  def onCompleted(flow: Flow[_]) {}
  // mark all throwables as handled
  def onThrowable(flow: Flow[_], throwable: Throwable): Boolean = true
}
