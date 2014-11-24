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

import scala.io.Source

import scala.util.Failure

import java.util.UUID

import scalaz.{Failure => _, _}, Scalaz._
import scalaz.\/-

import cascading.tap.Tap

import com.twitter.scalding.{Read, Args, Csv, Job, Execution}

import scalikejdbc.{SQL, AutoSession, ConnectionPool}

import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.core.Thermometer._

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl
import au.com.cba.omnia.parlour.flow.SqoopRiffle

class ExportSqoopSpec  extends ThermometerSpec with ExportDb { def is = s2"""
  Export Sqoop Flow/Job/Execution Spec
  ==========================

  end to end flow test                    ${flowTest.endToEndFlow}
  end to end job test                     ${jobTest.endToEndJob}
  end to end execution test               ${executionTest.endToEndExecution}
  execution test w/ no source             ${executionTest.endToEndExecutionNoSource}


  failing job returns false               ${jobTest.failingJob}
  job w/ exception throws                 ${jobTest.exceptionalJob}
  failing execution fails                 ${executionTest.failingExecution}

"""

  val resourceUrl = getClass.getResource("/sqoop")
  val exportDir = s"$dir/user/sales/books/customers"

  val oldData = Seq((1, "Hugo", "abc_accr", "Fish", "Tuna", 200))
  val newData = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/customer.txt").getLines()
                .map(parseData).toSeq

  def parseData(line: String): Customer = {
    val fields = line.split("\\|")
    (fields(0).toInt, fields(1), fields(2), fields(3), fields(4), fields(5).toInt)
  }

  def createDsl(table: String) = new ParlourExportDsl()
    .connectionString(connectionString)
    .username(username)
    .password(password)
    .exportDir(exportDir)
    .tableName(table)
    .numberOfMappers(1)
    .inputFieldsTerminatedBy('|')
    .hadoopMapRedHome(System.getProperty("user.home") + "/.ivy2/cache")


  object jobTest {
    def endToEndJob =
      withEnvironment(path(resourceUrl.toString)) {
        val expected = newData ++ oldData

        val table = tableSetup(oldData)
        val dsl = createDsl(table)

        val source = Csv(exportDir, "|").createTap(Read)
        val sink   = TableTap(dsl.toSqoopOptions)
        val job    = new ExportSqoopJob(dsl, source, sink)(scaldingArgs)

        job.runsOk
        tableData(table) must containTheSameElementsAs(expected)
      }

    def failingJob =
      withEnvironment(path(resourceUrl.toString)) {
        val dsl = createDsl("INVALID")

        val source   = Csv(exportDir).createTap(Read)
        val sink = TableTap(dsl.toSqoopOptions)
        val job    = new SquishExceptionsExportSqoopJob(dsl, source, sink)(scaldingArgs)
        (new VerifiableJob(job)).run must_== Some(s"Job failed to run <${job.name}>".left)
      }

    def exceptionalJob =
      withEnvironment(path(resourceUrl.toString)) {
        val dsl = createDsl("INVALID")

        val source   = Csv(exportDir).createTap(Read)
        val sink = TableTap(dsl.toSqoopOptions)
        val job    = new ExportSqoopJob(dsl, source, sink)(scaldingArgs)
        (new VerifiableJob(job)).run must beLike { case Some(\/-(_)) => ok }
      }
  }

  object flowTest {
    def endToEndFlow =
      withEnvironment(path(resourceUrl.toString)) {
        val expected = newData ++ oldData
        val table = tableSetup(oldData)
        val dsl = createDsl(table)

        val source = Csv(exportDir, "|").createTap(Read)
        val sink   = TableTap(dsl.toSqoopOptions)
        val flow   = new ExportSqoopFlow("endToEndFlow", dsl, Some(source), Some(sink))

        println(s"=========== endToEndFlow test running in $dir ===============")

        flow.complete
        flow.getFlowStats.isSuccessful must beTrue
        tableData(table) must containTheSameElementsAs(expected)
      }
  }

  object executionTest {
    def endToEndExecution =
      withEnvironment(path(resourceUrl.toString)) {
        val expected = newData ++ oldData

        val table = tableSetup(oldData)
        val dsl = createDsl(table)

        val source = Csv(exportDir, "|")
        val execution = SqoopExecution.sqoopExport(dsl, source)
        executesOk(execution)
        tableData(table) must containTheSameElementsAs(expected)
      }

    def endToEndExecutionNoSource =
      withEnvironment(path(resourceUrl.toString)) {
        val expected = newData ++ oldData

        val table = tableSetup(oldData)
        val dsl = createDsl(table)

        val execution = SqoopExecution.sqoopExport(dsl)
        executesOk(execution)
        tableData(table) must containTheSameElementsAs(expected)
      }

    def failingExecution =
      withEnvironment(path(resourceUrl.toString)) {
        val dsl = createDsl("INVALID")

        val execution = SqoopExecution.sqoopExport(dsl)
        execute(execution) must beLike { case Failure(_) => ok }
      }
  }

  class SquishExceptionsExportSqoopJob(
    options: ParlourExportOptions[_],
    source: Tap[_, _, _],
    sink: Tap[_, _, _])(
    args: Args
    ) extends ExportSqoopJob(options, source, sink)(args) {
    override def buildFlow = {
      val flow = super.buildFlow
      flow.addListener(new SquishExceptionListener)
      flow
    }
  }
}

trait ExportDb {
  Class.forName("org.hsqldb.jdbcDriver")

  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username = "sa"
  val password = ""
  val userHome = System.getProperty("user.home")

  implicit val session = AutoSession

  type Customer = (Int, String, String, String, String, Int)

  def tableSetup(data: Seq[Customer]): String = {
    val table = s"table_${UUID.randomUUID.toString.replace('-', '_')}"

    ConnectionPool.singleton(connectionString, username, password)

    SQL(s"""
      create table $table (
        id integer,
        name varchar(20),
        accr varchar(20),
        cat varchar(20),
        sub_cat varchar(20),
        balance integer
      )
    """).execute.apply()

    tableInsert(table, data)

    table
  }

  def tableInsert(table: String, data: Seq[Customer]) = {
    data.map { case (id, name, accr, cat, sub_cat, balance) =>
      SQL(s"""
        insert into $table
        values (?, ?, ?, ?, ?, ?)
      """).bind(id, name, accr, cat, sub_cat, balance).update.apply()
    }
  }

  def tableData(table: String): List[Customer] = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession
    SQL(s"select * from $table").map(rs => (rs.int("id"), rs.string("name"), rs.string("accr"),
      rs.string("cat"), rs.string("sub_cat"), rs.int("balance"))).list.apply()
  }
}
