//
//   Copyright 2014 Commonwealth Bank of Australia
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

import java.util.Properties

import com.cloudera.sqoop.SqoopOptions

import com.twitter.scalding.{Args, ArgsException, NullTap}

protected class NullTapWithId(id: String) extends NullTap {
  override def getIdentifier() = id
}
protected case class ExportDirTap(options: SqoopOptions) extends NullTapWithId(s"sqoop-export-dir[${options.getExportDir()}]")
protected case class TargetDirTap(options: SqoopOptions) extends NullTapWithId(s"sqoop-target-dir[${options.getTargetDir()}]")
protected case class TableTap(options: SqoopOptions) extends NullTapWithId(s"sqoop-table-name[${options.getTableName()}]")

object SqoopSyntax {
  def sqoopOptions(): SqoopOptions = new SqoopOptions()

  case class ParlourImportDsl(options: SqoopOptions = sqoopOptions()) extends ParlourDsl[ParlourImportDsl](options) with ParlourImportOptions[ParlourImportDsl]
  case class ParlourExportDsl(options: SqoopOptions = sqoopOptions()) extends ParlourDsl[ParlourExportDsl](options) with ParlourExportOptions[ParlourExportDsl]
  case class TeradataParlourImportDsl(options: SqoopOptions = sqoopOptions()) extends ParlourDsl[TeradataParlourImportDsl](options) with TeradataParlourImportOptions[TeradataParlourImportDsl]
  case class TeradataParlourExportDsl(options: SqoopOptions = sqoopOptions()) extends ParlourDsl[TeradataParlourExportDsl](options) with TeradataParlourExportOptions[TeradataParlourExportDsl]
}

class ParlourDsl[+Self <: ParlourDsl[_]](options: SqoopOptions) extends ParlourOptions[Self] {
  override def toSqoopOptions = options

  override protected def update(f: SqoopOptions => Unit) = {
    f(options)
    this.asInstanceOf[Self]
  }

  override protected def addExtraArgs(extraArgs: Array[String]) =
    update(options => {
      val args = options.getExtraArgs()
      options.setExtraArgs((if (args != null) options.getExtraArgs() else Array[String]()) ++ extraArgs)
    })
}

trait ParlourOptions[+Self <: ParlourOptions[_]] {
  val consoleArguments: ConsoleOptions = new ConsoleOptions(this)
  def toSqoopOptions: SqoopOptions

  protected def update(f: SqoopOptions => Unit): Self
  protected def addExtraArgs(extraArgs: Array[String]): Self

  /** Specify JDBC connect string */
  def connectionString(connectionString: String) = update(_.setConnectString(connectionString))
  consoleArguments.addOptional("connection-string", connectionString)

  /** Specify connection manager class to use */
  def connectionManager(className: String) = update(_.setConnManagerClassName(className))
  consoleArguments.addOptional("connection-manager", connectionManager)

  /** Manually specify JDBC driver class to use */
  def driver(className: String) = update(_.setDriverClassName(className))
  consoleArguments.addOptional("driver", driver)

  /**Set authentication username*/
  def username(username: String) = update(_.setUsername(username))
  consoleArguments.addOptional("username", username)

  /** Set authentication password */
  def password(password: String) = update(_.setPassword(password))
  consoleArguments.addOptional("password", password)

  /** Optional properties file that provides connection parameters */
  def connectionParams(params: Properties) = update(_.setConnectionParams(params))

  def numberOfMappers(count: Int) = update(_.setNumMappers(count))
  consoleArguments.addOptional("mappers", numberOfMappers, _.toInt)

  def tableName(tableName: String) = update(_.setTableName(tableName))
  consoleArguments.addOptional("table-name", tableName)

  def sqlQuery(sqlStatement: String) = update(_.setSqlQuery(sqlStatement))
  consoleArguments.addOptional("sql-query", sqlQuery)

  /** The string to be interpreted as null for string columns */
  def nullString(token: String) = update(_.setNullStringValue(token))
  consoleArguments.addOptional("null-string", nullString)

  /** The string to be interpreted as null for non-string columns */
  def nullNonString(token: String) = update(_.setNullNonStringValue(token))
  consoleArguments.addOptional("null-non-string", nullNonString)
}

trait ParlourExportOptions[+Self <: ParlourExportOptions[_]] extends ParlourOptions[Self] {
  /** HDFS source path for the export */
  def exportDir(dir: String) = update(_.setExportDir(dir))
  consoleArguments.addRequired("export-dir", exportDir)

  /** Sets a field enclosing character */
  def inputEnclosedBy(c: Char) = update(_.setInputEnclosedBy(c))
  consoleArguments.addOptional("input-enclosed-by", inputEnclosedBy, _.head)

  /** Sets the input escape character */
  def inputEscapedBy(c: Char) = update(_.setInputEscapedBy(c))
  consoleArguments.addOptional("input-escaped-by", inputEscapedBy, _.head)

  /** Sets the input field separator */
  def inputFieldsTerminatedBy(fieldsDelimiter: Char) = update(_.setInputFieldsTerminatedBy(fieldsDelimiter))
  consoleArguments.addOptional("input-fields-terminated-by", inputFieldsTerminatedBy, _.head, Some('|'))

  /** Sets the input end-of-line character */
  def inputLinesTerminatedBy(linesDelimiter: Char) = update(_.setInputLinesTerminatedBy(linesDelimiter))
  consoleArguments.addOptional("input-lines-terminated-by", inputLinesTerminatedBy, _.head, Some('\n'))

  /** Use batch mode for underlying statement execution. */
  def batch() = update(_.setBatchMode(true))
  consoleArguments.addBoolean("batch", batch)
}

trait ParlourImportOptions[+Self <: ParlourImportOptions[_]] extends ParlourOptions[Self] {
  /** Append data to an existing dataset in HDFS */
  def append() = update(_.setAppendMode(true))
  consoleArguments.addBoolean("append", append)

  /** Columns to import from table */
  def columns(cols: Array[String]) = update(_.setColumns(cols))
  consoleArguments.addOptional("columns", columns, str => str.split(","))

  /** Number of entries to read from database at once */
  def fetchSize(size: Int) = update(_.setFetchSize(size))
  consoleArguments.addOptional("fetch-size", fetchSize, _.toInt)

  /** Column of the table used to split work units */
  def splitBy(splitByColumn: String) = update(_.setSplitByCol(splitByColumn))
  consoleArguments.addOptional("split-by", splitBy)

  /** HDFS destination dir */
  def targetDir(dir: String) = update(_.setTargetDir(dir))
  consoleArguments.addRequired("target-dir", targetDir)

  /** WHERE clause to use during import */
  def where(conditions: String) = update(_.setWhereClause(conditions))
  consoleArguments.addOptional("where", where)

  /** Sets a field enclosing character */
  def enclosedBy(c: Char) = update(_.setEnclosedBy(c))
  consoleArguments.addOptional("enclosed-by", enclosedBy, _.head)

  /** Sets the escape character */
  def escapedBy(c: Char) = update(_.setEscapedBy(c))
  consoleArguments.addOptional("escaped-by", escapedBy, _.head)

  /** Sets the field separator character */
  def fieldsTerminatedBy(c: Char) = update(_.setFieldsTerminatedBy(c))
  consoleArguments.addOptional("fields-terminated-by", fieldsTerminatedBy, _.head, Some('|'))

  /** Sets the end-of-line character */
  def linesTerminatedBy(c: Char) = update(_.setLinesTerminatedBy(c))
  consoleArguments.addOptional("lines-terminated-by", linesTerminatedBy, _.head, Some('\n'))
}

/**
 * Options specific to the Teradata Connector
 * See: http://www.cloudera.com/content/cloudera-content/cloudera-docs/Connectors/Teradata/Cloudera-Connector-for-Teradata/cctd_use_tpcc.html
 */
trait TeradataParlourOptions[+Self <: TeradataParlourOptions[_]] extends ParlourOptions[Self] {
  update(_.setConnManagerClassName("com.cloudera.connector.teradata.TeradataManager"))

  /** Override default staging table name. Please note that this parameter applies only in case that staging tables are used during the data transfer. */
  def stagingTable(tableName: String) = addExtraArgs(Array("--staging-table", tableName))
  consoleArguments.addOptional("teradata-staging-table", stagingTable)

  /** Override default staging database name. Please note that this parameter applies only in case that staging tables are used during the data transfer. */
  def stagingDatabase(databaseName: String) = addExtraArgs(Array("--staging-database", databaseName: String))
  consoleArguments.addOptional("teradata-staging-database", stagingDatabase)

  /** Specifies number of rows that will be procesed together in one batch. */
  def batchSize(size: Int) = addExtraArgs(Array("--batch-size", size.toString))
  consoleArguments.addOptional("teradata-batch-size", batchSize, _.toInt)

  /** Force the connector to create the staging table if input/output method support the staging tables. */
  def forceStaging() = addExtraArgs(Array("--force-staging"))
  consoleArguments.addBoolean("teradata-force-staging", forceStaging)

  /**
   * Allows arbitrary query bands to be set for all queries that are executed by the connector. The expected format is a semicolon-separated key=value pair list.
   * Note that a final semicolon is required after the last key=value pair as well. For example, Data_Center=XO;Location=Europe;.
   */
  def queryBand(bandPairs: String) = addExtraArgs(Array("--query-band", bandPairs))
  consoleArguments.addOptional("teradata-query-band", queryBand)

  /** By default, the connector will use Teradata system views to obtain metadata. Using this parameter the connector will switch to XViews instead. */
  def skipXviews() = addExtraArgs(Array("--skip-xview"))
  consoleArguments.addBoolean("teradata-skip-xview", skipXviews)
}

sealed trait TeradataInputMethod
case object SplitByAmp extends TeradataInputMethod
case object SplitByValue extends TeradataInputMethod
case object SplitByPartition extends TeradataInputMethod
case object SplitByHash extends TeradataInputMethod

/** Options specific to the Teradata Connector when importing */
trait TeradataParlourImportOptions[+Self <: TeradataParlourImportOptions[_]] extends TeradataParlourOptions[Self] with ParlourImportOptions[Self] {
  /**
   * Specifies which input method should be used to transfer data from Teradata to Hadoop.
   */
  def inputMethod(method: TeradataInputMethod) = addExtraArgs(Array("--input-method", method match {
    case SplitByAmp       => "split.by.amp"
    case SplitByValue     => "split.by.value"
    case SplitByPartition => "split.by.partition"
    case SplitByHash      => "split.by.hash"
  }))
  consoleArguments.addOptional("teradata-input-method", inputMethod, _ match {
    case "split.by.amp"       => SplitByAmp
    case "split.by.value"     => SplitByValue
    case "split.by.partition" => SplitByPartition
    case "split.by.hash"      => SplitByHash
    case _                    => throw new ArgsException("Please provide a valid input method, one of split.by.[amp|value|partition|hash]")
  })

  /** Access lock is used to improve concurrency. When used, the import job will not be blocked by concurrent accesses to the same table. */
  def accessLock() = addExtraArgs(Array("--access-lock"))
  consoleArguments.addBoolean("teradata-access-lock", accessLock)

  /**
   * By default, the connector will drop all automatically created staging tables upon failed export.
   * Using this option will leave the staging tables with partially imported data in the database.
   */
  def keepStagingTable() = addExtraArgs(Array("--keep-staging-table"))
  consoleArguments.addBoolean("teradata-keep-staging-table", keepStagingTable)

  /**
   * Number of partitions that should be used for the automatically created staging table.
   * Note that the connector will automatically generate the value based on the number of mappers used for the job. (only for split.by.partition)
   */
  def numPartitionsForStagingTable(numPartitions: Int) = addExtraArgs(Array("--num-partitions-for-staging-table", numPartitions.toString))
  consoleArguments.addOptional("teradata-num-partitions-for-staging-table", numPartitionsForStagingTable, _.toInt)
}

sealed trait TeradataOutputMethod
case object BatchInsert extends TeradataOutputMethod
case object InternalFastload extends TeradataOutputMethod
case object MultipleFastload extends TeradataOutputMethod

/** Options specific to the Teradata Connector when exporting */
trait TeradataParlourExportOptions[+Self <: TeradataParlourExportOptions[_]] extends TeradataParlourOptions[Self] with ParlourExportOptions[Self] {
  /** Specifies which output method whould be used to transfer data from Hadoop to Teradata. */

  def outputMethod(method: TeradataOutputMethod) = addExtraArgs(Array("--output-method", method match {
    case BatchInsert      => "batch.insert"
    case InternalFastload => "internal.fastload"
    case MultipleFastload => "multiple.fastload"
  }))
  consoleArguments.addOptional("teradata-output-method", outputMethod, _ match {
    case "batch.insert"      => BatchInsert
    case "internal.fastload" => InternalFastload
    case "multiple.fastload" => MultipleFastload
    case _                   => throw new ArgsException("Please provide a valid output method, one of [batch.insert|internal.fastload|multiple.fastload]")
  })

  /** Specifies a prefix for created error tables. (only for internal.fastload) */
  def errorTable(tableName: String) = addExtraArgs(Array("--error-table", tableName))
  consoleArguments.addOptional("teradata-error-table", errorTable)

  /**
   * Hostname or IP address of the node where you are executing Sqoop, one that is visible from the Hadoop cluster. The connector has the
   * ability to autodetect the interface. This parameter has been provided to override the autodection routine. (only for internal.fastload)
   */
  def fastloadSocketHostName(hostname: String) = addExtraArgs(Array("--fastload-socket-hostname", hostname))
  consoleArguments.addOptional("teradata-fastload-socket-hostname", fastloadSocketHostName)
}

class ConsoleOptions(options: ParlourOptions[_]) {
  type POptions = ParlourOptions[_]
  val consoleOptions = scala.collection.mutable.Map[String, (Args) => Unit]()

  def addRequired(name: String, setter: String => POptions): ConsoleOptions = {
    addRequired(name, setter, v => v)
    this
  }

  def addRequired[T](name: String, setter: T => POptions, converter: String => T): ConsoleOptions = {
    consoleOptions += (name, (args: Args) => setter(converter(args(name))))
    this
  }

  def addBoolean(name: String, setter: () => POptions): ConsoleOptions = {
    consoleOptions += (name, (args: Args) => if (args.boolean(name)) setter())
    this
  }

  def addOptional(name: String, setter: String => POptions, default: Option[String] = None): ConsoleOptions = {
    addOptional(name, setter, v => v, default)
    this
  }

  def addOptional[T](name: String, setter: T => POptions, converter: String => T, default: Option[T] = None): ConsoleOptions = {
    consoleOptions += (name, (args: Args) => {
      args.optional(name) match {
        case Some(value) => setter(converter(value))
        case None        => default.foreach(setter)
      }
    })
    this
  }

  def setOptions(args: Args): ParlourOptions[_] = {
    consoleOptions.values.foreach(setter => setter(args))
    options
  }

  def usage: String = {
    consoleOptions.map(_ match {
      case (name, setter) => s"--${name}"
    }).mkString("\n")
  }
}
