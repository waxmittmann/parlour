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
import java.sql.Date
import java.math.BigDecimal

import com.twitter.scalding.{Csv, Execution}

import scalikejdbc.{SQL, AutoSession, ConnectionPool}

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

class ExportSqoopSpec  extends ThermometerSpec with ExportDb { def is = s2"""
  Export Sqoop Flow/Job/Execution Spec
  ==========================

  end to end console test                 endToEndConsole
  end to end execution test               endToEndExecution
  execution test w/ no source             endToEndExecutionNoSource
  execution test using Parquet            $endToEndExecutionParquet
  null strings are correctly handled      nullExecution
  failing execution fails                 failingExecution

"""

  val resourceUrl = getClass.getResource("/sqoop")
  val exportDir = s"/Users/shoermann/workspace/parlour/src/test/resources/parquet/output"

  val oldData = Seq((1, Option("Hugo"), "abc_accr", "Fish", "Tuna", 200))
  val newData = Source.fromFile(s"${resourceUrl.getPath}/sales/books/customers/customer.txt").getLines()
                .map(parseData).toSeq

  def parseData(line: String): Customer = {
    val fields = line.split("\\|")
    (fields(0).toInt, Option(fields(1)), fields(2), fields(3), fields(4), fields(5).toInt)
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

  /*def endToEndConsole = withEnvironment(path(resourceUrl.toString)) {
    val expected = newData ++ oldData
    val table    = tableSetup(oldData)
    val dsl      = createDsl(table)
    val args     = Map(
      "connection-string"     -> connectionString,
      "username"              -> username,
      "password"              -> password,
      "table-name"            -> table,
      "export-dir"            -> exportDir,
      "mappers"               -> "1",
      "input-field-delimiter" -> "|",
      "hadoop-mapred-home"    -> dsl.getHadoopMapRedHome.get
    )

    executesOk(ExportSqoopConsoleJob.job, args.mapValues(List(_)))
    tableData(table) must containTheSameElementsAs(expected)
  }
  def endToEndExecution = withEnvironment(path(resourceUrl.toString)) {
    val expected = newData ++ oldData
    val table    = tableSetup(oldData)
    val dsl      = createDsl(table)
    val source   = Csv(exportDir, "|")

    executesOk(SqoopExecution.sqoopExport(dsl, source))
    tableData(table) must containTheSameElementsAs(expected)
  }*/

  def endToEndExecutionParquet = {
    val expected = List(("abc", "Batman", "100000", 1440252000000l))
    val table    = tableSetup()
    val dsl      = createDsl(table)

    executesOk(SqoopExecution.sqoopExport(dsl))
    tableData(table) must containTheSameElementsAs(expected)
  }


  /*
  def endToEndExecutionNoSource = withEnvironment(path(resourceUrl.toString)) {
    val expected = newData ++ oldData
    val table    = tableSetup(oldData)
    val dsl      = createDsl(table)

    executesOk(SqoopExecution.sqoopExport(dsl))
    tableData(table) must containTheSameElementsAs(expected)
  }

  def failingExecution = withEnvironment(path(resourceUrl.toString)) {
    val dsl = createDsl("INVALID")

    val execution = SqoopExecution.sqoopExport(dsl)
    execute(execution) must beLike { case Failure(_) => ok }
  }

  def nullExecution = withEnvironment(path(resourceUrl.toString)) {
    val nullDataOut = Seq((3, Option.empty[String], "002", "F", "M", 225))
    val expected    = newData.init ++ oldData ++ nullDataOut
    val table       = tableSetup(oldData)
    val dsl         = createDsl(table).inputNull("Bart")

    executesOk(SqoopExecution.sqoopExport(dsl))
    tableData(table) must containTheSameElementsAs(expected)
  }*/
}

trait ExportDb {
  Class.forName("org.hsqldb.jdbcDriver")

  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username         = "sa"
  val password         = ""
  val userHome         = System.getProperty("user.home")

  implicit val session = AutoSession

  type Customer = (Int, Option[String], String, String, String, Int)

  def tableSetup(): String = {
    val table = s"table_${UUID.randomUUID.toString.replace('-', '_')}"

    ConnectionPool.singleton(connectionString, username, password)

    SQL(s"""
      create table $table (
        id varchar(20),
        customer varchar(20),
        balance decimal,
        balance_cents date
      )
    """).execute.apply()

    table
  }

  def tableData(table: String): List[(String, String, BigDecimal, Date)] = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession
    SQL(s"select * from $table").map(rs => (rs.string("id"), rs.string("customer"), rs.bigDecimal("balance"),
      rs.date("balance_cents"))).list.apply()
  }
}
