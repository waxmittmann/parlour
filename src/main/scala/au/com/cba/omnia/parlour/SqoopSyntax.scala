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

import com.twitter.scalding._

import org.apache.hadoop.conf.Configuration

import au.com.cba.omnia.parlour.SqoopSyntax._

/**
 * A directory representation that help with Source/Sink match based on merely the path. The `TextLineScheme`
 * although mixed in, would be ignored as sqoop handles paths directly i.e. not through any passed in `Tap`.
 * @param path: Path to the directory on interest
 */
class DirSource(path: String) extends FixedPathSource(path) with TextLineScheme

protected class NullTapWithId(id: String) extends NullTap {
  override def getIdentifier() = id
}

protected case class TableTap(options: SqoopOptions) extends NullTapWithId(options.getTableName())
// ExportDirTap and TargetDirTap are used merely as placeholders for a Tap match based on path during a cascade.
// The TextLineScheme will be ignore as Sqoop directly handles the file/path in question
protected case class ExportDirTap(options: SqoopOptions) extends DirSource(options.getExportDir())
protected case class TargetDirTap(options: SqoopOptions) extends DirSource(options.getTargetDir())


object SqoopSyntax {
  type SqoopModifier = SqoopOptions => Unit

  case class ParlourImportDsl(updates: List[SqoopModifier] = List()) extends ParlourDsl[ParlourImportDsl] with ParlourImportOptions[ParlourImportDsl]
  case class ParlourExportDsl(updates: List[SqoopModifier] = List()) extends ParlourDsl[ParlourExportDsl] with ParlourExportOptions[ParlourExportDsl]
  case class TeradataParlourImportDsl(updates: List[SqoopModifier] = List()) extends ParlourDsl[TeradataParlourImportDsl] with TeradataParlourImportOptions[TeradataParlourImportDsl]
  case class TeradataParlourExportDsl(updates: List[SqoopModifier] = List()) extends ParlourDsl[TeradataParlourExportDsl] with TeradataParlourExportOptions[TeradataParlourExportDsl]

}


sealed trait ParlourDsl[+Self <: ParlourDsl[_]] {
  def defaultOptions: List[SqoopModifier]
  def updates: List[SqoopModifier]

  def toSqoopOptions = {
    //foldRight instead of foldLeft to enable options overriding
    (updates ++ defaultOptions).foldRight(new SqoopOptions) {
      (f, opts) =>
        f(opts)
        opts
    }
  }

  def update(f: SqoopModifier): Self = {
    val nextUpdates = f :: updates

    val nextDsl = this.asInstanceOf[Self] match {
      case ParlourImportDsl(_) => ParlourImportDsl(nextUpdates)
      case ParlourExportDsl(_) => ParlourExportDsl(nextUpdates)
      case TeradataParlourImportDsl(_) => TeradataParlourImportDsl(nextUpdates)
      case TeradataParlourExportDsl(_) => TeradataParlourExportDsl(nextUpdates)
    }
    nextDsl.asInstanceOf[Self]
  }

  protected def addExtraArgs(extraArgs: Array[String]) =
    update(addExtraArgsRaw(extraArgs))
  protected def addExtraArgsRaw(extraArgs: Array[String])(options: SqoopOptions): Unit = {
    val args = options.getExtraArgs()
    options.setExtraArgs((if (args != null) options.getExtraArgs() else Array[String]()) ++ extraArgs)
  }
}

trait ParlourOptions[+Self <: ParlourOptions[_]] extends ParlourDsl[Self] with ConsoleOptions[Self] {
  override def defaultOptions: List[SqoopModifier] = {
    List[SqoopModifier](_.setSkipDistCache(false), _.setVerbose(false))
  }

  /** Specify JDBC connect string */
  def connectionString(connectionString: String) = update(_.setConnectString(connectionString))
  addOptional("connection-string", (v: String) => so => so.setConnectString(v))
  def getConnectionString = Option(toSqoopOptions.getConnectString)

  /** Specify connection manager class to use */
  def connectionManager(className: String) = update(_.setConnManagerClassName(className))
  addOptional("connection-manager", (v: String) => so => so.setConnManagerClassName(v))
  def getConnectionManager = Option(toSqoopOptions.getConnManagerClassName)

  /** Manually specify JDBC driver class to use */
  def driver(className: String) = update(_.setDriverClassName(className))
  addOptional("driver", (v: String) => so => so.setDriverClassName(v))
  def getDriver = Option(toSqoopOptions.getDriverClassName)

  /**Set authentication username*/
  def username(username: String) = update(_.setUsername(username))
  addOptional("username", (v: String) => so => so.setUsername(v))
  def getUsername = Option(toSqoopOptions.getUsername)

  /** Set authentication password */
  def password(password: String) = update(_.setPassword(password))
  addOptional("password", (v: String) => so => so.setPassword(v))
  def getPassword = Option(toSqoopOptions.getPassword)

  /** Optional properties file that provides connection parameters */
  def connectionParams(params: Properties) = update(_.setConnectionParams(params))
  def getConnectionParams = Option(toSqoopOptions.getConnectionParams)

  def numberOfMappers(count: Int) = update(_.setNumMappers(count))
  addOptional("mappers", (v: Int) => so => so.setNumMappers(v), _.toInt)
  def getNumberOfMappers = Option(toSqoopOptions.getNumMappers)

  def tableName(tableName: String) = update(_.setTableName(tableName))
  addOptional("table-name", (v: String) => so => so.setTableName(v))
  def getTableName = Option(toSqoopOptions.getTableName)

  def sqlQuery(sqlStatement: String) = update(_.setSqlQuery(sqlStatement))
  addOptional("sql-query", (v: String) => so => so.setSqlQuery(v))
  def getSqlQuery = Option(toSqoopOptions.getSqlQuery)

  /** The string to be interpreted as null for string columns */
  def nullString(token: String) = update(_.setNullStringValue(token))
  addOptional("null-string", (v: String) => so => so.setNullStringValue(v))
  def getNullString = Option(toSqoopOptions.getNullStringValue)

  /** The string to be interpreted as null for non-string columns */
  def nullNonString(token: String) = update(_.setNullNonStringValue(token))
  addOptional("null-non-string", (v: String) => so => so.setNullNonStringValue(v))
  def getNullNonString = Option(toSqoopOptions.getNullNonStringValue)

  /** Use verbose mode - This doesn't really work */
  def verbose() = update(_.setVerbose(true))
  addBoolean("verbose", () => so => so.setVerbose(true))
  def getVerbose = Option(toSqoopOptions.getVerbose)

  /** The path to hadoop mapred home - used by tests*/
  def hadoopMapRedHome(home: String) = update(_.setHadoopMapRedHome(home))
  addOptional("hadoop-mapred-home", (v: String) => so => so.setHadoopMapRedHome(v))
  def getHadoopMapRedHome = Option(toSqoopOptions.getHadoopMapRedHome)

  /** Can skip the distributed cache feature of sqoop */
  def skipDistCache() = update(_.setSkipDistCache(true))
  addBoolean("skip-dist-cache", () => so => so.setSkipDistCache(true))
  def getSkipDistCache = Option(toSqoopOptions.isSkipDistCache)

  /** Columns to import/export from/to table */
  def columns(cols: Array[String]) = update(_.setColumns(cols))
  addOptional("columns", (v: Array[String]) => so => so.setColumns(v), str => str.split(","))
  def getColumns = Option(toSqoopOptions.getColumns)

  /** Hadoop Configuration */
  private[parlour] def config(config: Configuration) = update(_.setConf(config))
  private[parlour] def getConfig = Option(toSqoopOptions.getConf)
}

trait ParlourExportOptions[+Self <: ParlourExportOptions[_]] extends ParlourOptions[Self] {
  /** HDFS source path for the export */
  def exportDir(dir: String) = update(_.setExportDir(dir))
  addRequired("export-dir", (v: String) => so => so.setExportDir(v))
  def getExportDir = Option(toSqoopOptions.getExportDir)

  /** Sets a field enclosing character */
  def inputEnclosedBy(c: Char) = update(_.setInputEnclosedBy(c))
  addOptional("input-enclosed-by", (v: Char) => so => so.setInputEnclosedBy(v), _.head)
  def getInputEnclosedBy = Option(toSqoopOptions.getInputEnclosedBy)

  /** Sets the input escape character */
  def inputEscapedBy(c: Char) = update(_.setInputEscapedBy(c))
  addOptional("input-escaped-by", (v: Char) => so => so.setInputEscapedBy(v), _.head)
  def getInputEscapedBy = Option(toSqoopOptions.getInputEscapedBy)

  /** Sets the input field separator */
  def inputFieldsTerminatedBy(fieldsDelimiter: Char) = update(_.setInputFieldsTerminatedBy(fieldsDelimiter))
  addOptional("input-fields-terminated-by", (v: Char) => so => so.setInputFieldsTerminatedBy(v), _.head, Some('|'))
  def getInputFieldsTerminatedBy = Option(toSqoopOptions.getInputFieldDelim)

  /** Sets the input end-of-line character */
  def inputLinesTerminatedBy(linesDelimiter: Char) = update(_.setInputLinesTerminatedBy(linesDelimiter))
  addOptional("input-lines-terminated-by", (v: Char) => so => so.setInputLinesTerminatedBy(v), _.head, Some('\n'))
  def getInputLinesTerminatedBy = Option(toSqoopOptions.getInputRecordDelim)

  /** Use batch mode for underlying statement execution. */
  def batch() = update(_.setBatchMode(true))
  addBoolean("batch", () => so => so.setBatchMode(true))
  def getBatch() = Option(toSqoopOptions.isBatchMode)

  /** The string to be interpreted as null for input string columns */
  def inputNullString(token: String) = update(_.setInNullStringValue(token))
  addOptional("input-null-string", (v: String) => so => so.setInNullStringValue(v))
  def getInputNullString = Option(toSqoopOptions.getInNullStringValue)

  /** The string to be interpreted as null for input non-string columns */
  def inputNullNonString(token: String) = update(_.setInNullNonStringValue(token))
  addOptional("input-null-non-string", (v: String) => so => so.setInNullNonStringValue(v))
  def getInputNullNonString = Option(toSqoopOptions.getInNullNonStringValue)

  /** The string to be interpreted as null for input string and input non-string columns */
  def inputNull(token: String) = {
    inputNullString(token)
    inputNullNonString(token)
  }
}

trait ParlourImportOptions[+Self <: ParlourImportOptions[_]] extends ParlourOptions[Self] {
  /** Append data to an existing dataset in HDFS */
  def append() = update(_.setAppendMode(true))
  addBoolean("append", () => so => so.setAppendMode(true))
  def getAppend = Option(toSqoopOptions.isAppendMode)

  /** Number of entries to read from database at once */
  def fetchSize(size: Int) = update(_.setFetchSize(size))
  addOptional("fetch-size", (v: Int) => so => so.setFetchSize(v), _.toInt)
  def getFetchSize = Option(toSqoopOptions.getFetchSize)

  /** Column of the table used to split work units */
  def splitBy(splitByColumn: String) = update(_.setSplitByCol(splitByColumn))
  addOptional("split-by", (v: String) => so => so.setSplitByCol(v))
  def getSplitBy = Option(toSqoopOptions.getSplitByCol)

  /** HDFS destination dir */
  def targetDir(dir: String) = update(_.setTargetDir(dir))
  addRequired("target-dir", (v: String) => so => so.setTargetDir(v))
  def getTargetDir = Option(toSqoopOptions.getTargetDir)

  /** WHERE clause to use during import */
  def where(conditions: String) = update(_.setWhereClause(conditions))
  addOptional("where", (v: String) => so => so.setWhereClause(v))
  def getWhere = Option(toSqoopOptions.getWhereClause)

  /** Sets a field enclosing character */
  def enclosedBy(c: Char) = update(_.setEnclosedBy(c))
  addOptional("enclosed-by", (v: Char) => so => so.setEnclosedBy(v), _.head)
  def getEnclosedBy = Option(toSqoopOptions.getOutputEnclosedBy)

  /** Sets the escape character */
  def escapedBy(c: Char) = update(_.setEscapedBy(c))
  addOptional("escaped-by", (v: Char) => so => so.setEscapedBy(v), _.head)
  def getEscapedBy = Option(toSqoopOptions.getOutputEscapedBy)

  /** Sets the field separator character */
  def fieldsTerminatedBy(c: Char) = update(_.setFieldsTerminatedBy(c))
  addOptional("fields-terminated-by", (v: Char) => so => so.setFieldsTerminatedBy(v), _.head, Some('|'))
  def getFieldTerminatedBy = Option(toSqoopOptions.getOutputFieldDelim)

  /** Sets the end-of-line character */
  def linesTerminatedBy(c: Char) = update(_.setLinesTerminatedBy(c))
  addOptional("lines-terminated-by", (v: Char) => so => so.setLinesTerminatedBy(v), _.head, Some('\n'))
  def getLinesTerminatedBy = Option(toSqoopOptions.getOutputRecordDelim)
}

/**
 * Options specific to the Teradata Connector
 * See: http://www.cloudera.com/content/cloudera-content/cloudera-docs/Connectors/Teradata/Cloudera-Connector-for-Teradata/cctd_use_tpcc.html
 */
trait TeradataParlourOptions[+Self <: TeradataParlourOptions[_]] extends ParlourOptions[Self] {
  import TeradataParlourOptions._

  override def defaultOptions: List[SqoopModifier] = {
    super.defaultOptions ++ List[SqoopModifier](_.setConnManagerClassName("com.cloudera.connector.teradata.TeradataManager"))
  }

  protected def getExtraBooleanArg(name: String): Option[Boolean] = Option(toSqoopOptions.getExtraArgs.find(_ == name).isDefined)

  protected def getExtraValueArg(name: String): Option[String] = {
    val so = toSqoopOptions
    if (so.getExtraArgs.size < 2) None
    else {
      so.getExtraArgs.sliding(2).find(_(0) == name).map(_(1))
    }
  }

  /** Override default staging table name. Please note that this parameter applies only in case that staging tables are used during the data transfer. */
  def stagingTable(tableName: String) = addExtraArgs(Array(STAGING_TABLE, tableName))
  addOptional("teradata-staging-table", (v: String) => so => addExtraArgsRaw(Array(STAGING_TABLE, v))(so))
  def getStagingTable = getExtraValueArg(STAGING_TABLE)

  /** Override default staging database name. Please note that this parameter applies only in case that staging tables are used during the data transfer. */
  def stagingDatabase(databaseName: String) = addExtraArgs(Array(STAGING_DATABASE, databaseName: String))
  addOptional("teradata-staging-database", (v: String) => so => addExtraArgsRaw(Array(STAGING_DATABASE, v))(so))
  def getStagingDatabase = getExtraValueArg(STAGING_DATABASE)

  /** Specifies number of rows that will be procesed together in one batch. */
  def batchSize(size: Int) = addExtraArgs(Array(BATCH_SIZE, size.toString))
  addOptional("teradata-batch-size", (v: Int) => so => addExtraArgsRaw(Array(BATCH_SIZE, v.toString))(so), _.toInt)
  def getBatchSize = getExtraValueArg(BATCH_SIZE)

  /** Force the connector to create the staging table if input/output method support the staging tables. */
  def forceStaging() = addExtraArgs(Array(FORCE_STAGING))
  addBoolean("teradata-force-staging", () => so => addExtraArgsRaw(Array(FORCE_STAGING))(so))
  def getForceStaging = getExtraBooleanArg(FORCE_STAGING)

  /**
   * Allows arbitrary query bands to be set for all queries that are executed by the connector. The expected format is a semicolon-separated key=value pair list.
   * Note that a final semicolon is required after the last key=value pair as well. For example, Data_Center=XO;Location=Europe;.
   */
  def queryBand(bandPairs: String) = addExtraArgs(Array(QUERY_BAND, bandPairs))
  addOptional("teradata-query-band", (v: String) => so => addExtraArgsRaw(Array(QUERY_BAND, v))(so))
  def getQueryBand = getExtraValueArg(QUERY_BAND)

  /** By default, the connector will use Teradata system views to obtain metadata. Using this parameter the connector will switch to XViews instead. */
  def skipXviews() = addExtraArgs(Array(SKIP_VIEW))
  addBoolean("teradata-skip-xview", () => so => addExtraArgsRaw(Array(SKIP_VIEW))(so))
  def getSkipXviews = getExtraBooleanArg(SKIP_VIEW)
}

object TeradataParlourOptions {
  val STAGING_TABLE     = "--staging-table"
  val STAGING_DATABASE  = "--staging-database"
  val BATCH_SIZE        = "--batch-size"
  val FORCE_STAGING     = "--force-staging"
  val QUERY_BAND        = "--query-band"
  val SKIP_VIEW         = "--skip-xview"
}

sealed trait TeradataInputMethod
case object SplitByAmp extends TeradataInputMethod
case object SplitByValue extends TeradataInputMethod
case object SplitByPartition extends TeradataInputMethod
case object SplitByHash extends TeradataInputMethod

object TeradataInputMethod {
  def toString(method: TeradataInputMethod) = method match {
    case SplitByAmp       => "split.by.amp"
    case SplitByValue     => "split.by.value"
    case SplitByPartition => "split.by.partition"
    case SplitByHash      => "split.by.hash"
  }

  def fromString(method: String) = Option(method match {
    case "split.by.amp"       => SplitByAmp
    case "split.by.value"     => SplitByValue
    case "split.by.partition" => SplitByPartition
    case "split.by.hash"      => SplitByHash
    case _                    => null
  })
}

/** Options specific to the Teradata Connector when importing */
trait TeradataParlourImportOptions[+Self <: TeradataParlourImportOptions[_]] extends TeradataParlourOptions[Self] with ParlourImportOptions[Self] {
  import TeradataParlourImportOptions._
  /**
   * Specifies which input method should be used to transfer data from Teradata to Hadoop.
   */
  def inputMethod(method: TeradataInputMethod) = addExtraArgs(Array(INPUT_METHOD, TeradataInputMethod.toString(method)))
  addOptional("teradata-input-method",
    (v:TeradataInputMethod) => so => addExtraArgsRaw(Array(INPUT_METHOD,
    TeradataInputMethod.toString(v)))(so),
    TeradataInputMethod.fromString(_).getOrElse(throw new ArgsException("Please provide a valid input method, one of split.by.[amp|value|partition|hash]"))
  )
  def getInputMethod = getExtraValueArg(INPUT_METHOD).map(TeradataInputMethod.fromString)

  /** Access lock is used to improve concurrency. When used, the import job will not be blocked by concurrent accesses to the same table. */
  def accessLock() = addExtraArgs(Array(ACCESS_LOCK))
  addBoolean("teradata-access-lock", () => so => addExtraArgsRaw(Array(ACCESS_LOCK))(so))
  def getAccessLock = getExtraBooleanArg(ACCESS_LOCK)

  /**
   * By default, the connector will drop all automatically created staging tables upon failed export.
   * Using this option will leave the staging tables with partially imported data in the database.
   */
  def keepStagingTable() = addExtraArgs(Array(KEEP_STAGING_TABLE))
  addBoolean("teradata-keep-staging-table", () => so => addExtraArgsRaw(Array(KEEP_STAGING_TABLE))(so))
  def getKeepStagingTable = getExtraBooleanArg(KEEP_STAGING_TABLE)

  /**
   * Number of partitions that should be used for the automatically created staging table.
   * Note that the connector will automatically generate the value based on the number of mappers used for the job. (only for split.by.partition)
   */
  def numPartitionsForStagingTable(numPartitions: Int) = addExtraArgs(Array(NUM_PARTITIONS_FOR_STAGING_TABLE, numPartitions.toString))
  addOptional("teradata-num-partitions-for-staging-table", (v: Int) => so => addExtraArgsRaw(Array(NUM_PARTITIONS_FOR_STAGING_TABLE, v.toString))(so), _.toInt)
  def getNumPartitionsForStagingTable = getExtraValueArg(NUM_PARTITIONS_FOR_STAGING_TABLE).map(_.toInt)
}

object TeradataParlourImportOptions {
  val INPUT_METHOD                      = "--input-method"
  val ACCESS_LOCK                       = "--access-lock"
  val KEEP_STAGING_TABLE                = "--keep-staging-table"
  val NUM_PARTITIONS_FOR_STAGING_TABLE  = "--num-partitions-for-staging-table"
}

sealed trait TeradataOutputMethod
case object BatchInsert extends TeradataOutputMethod
case object InternalFastload extends TeradataOutputMethod
case object MultipleFastload extends TeradataOutputMethod

object TeradataOutputMethod {
  def toString(method: TeradataOutputMethod): String = method match {
    case BatchInsert      => "batch.insert"
    case InternalFastload => "internal.fastload"
    case MultipleFastload => "multiple.fastload"
  }

  def fromString(method: String) = Option(method match {
    case "batch.insert"       => BatchInsert
    case "internal.fastload"  => InternalFastload
    case "multiple.fastload"  => MultipleFastload
    case _                    => null
  })
}

/** Options specific to the Teradata Connector when exporting */
trait TeradataParlourExportOptions[+Self <: TeradataParlourExportOptions[_]] extends TeradataParlourOptions[Self] with ParlourExportOptions[Self] {
  import TeradataParlourExportOptions._
  /** Specifies which output method whould be used to transfer data from Hadoop to Teradata. */

  def outputMethod(method: TeradataOutputMethod) = addExtraArgs(Array(OUTPUT_METHOD, TeradataOutputMethod.toString(method)))
  addOptional("teradata-output-method",
    (v: TeradataOutputMethod) => so => addExtraArgsRaw(Array(OUTPUT_METHOD,
    TeradataOutputMethod.toString(v)))(so),
    TeradataOutputMethod.fromString(_).getOrElse(throw new ArgsException("Please provide a valid output method, one of [batch.insert|internal.fastload|multiple.fastload]"))
  )
  def getOutputMethod = getExtraValueArg(OUTPUT_METHOD).map(TeradataOutputMethod.fromString)

  /** Specifies a prefix for created error tables. (only for internal.fastload) */
  def errorTable(tableName: String) = addExtraArgs(Array(ERROR_TABLE, tableName))
  addOptional("teradata-error-table", (v: String) => so => addExtraArgsRaw(Array(ERROR_TABLE, v))(so))
  def getErrorTable = getExtraValueArg(ERROR_TABLE)

  /**
   * Hostname or IP address of the node where you are executing Sqoop, one that is visible from the Hadoop cluster. The connector has the
   * ability to autodetect the interface. This parameter has been provided to override the autodection routine. (only for internal.fastload)
   */
  def fastloadSocketHostName(hostname: String) = addExtraArgs(Array(FASTLOAD_SOCKET_HOSTNAME, hostname))
  addOptional("teradata-fastload-socket-hostname", (v: String) => so => addExtraArgsRaw(Array(FASTLOAD_SOCKET_HOSTNAME, v))(so))
  def getFastloadSocketHostName = getExtraValueArg(FASTLOAD_SOCKET_HOSTNAME)
}

object TeradataParlourExportOptions {
  val OUTPUT_METHOD             = "--output-method"
  val ERROR_TABLE               = "--error-table"
  val FASTLOAD_SOCKET_HOSTNAME  = "--fastload-socket-hostname"
}

trait ConsoleOptions[+Self <: ParlourOptions[_]] {
  self: ParlourOptions[Self]  =>

  import SqoopSyntax._
  type ConsoleSqoopModifier = Args => SqoopModifier

  val mods = scala.collection.mutable.Map[String, ConsoleSqoopModifier]()

  def addRequired(name: String, setter: String => SqoopModifier): ConsoleOptions[Self] = {
    addRequired(name, setter, v => v)
    this
  }

  def addRequired[T](name: String, setter: T => SqoopModifier, converter: String => T): ConsoleOptions[Self] = {
    mods.put(name, ((args: Args) => converter(args(name))) andThen setter)
    this
  }

  def addBoolean(name: String, setter: () => SqoopOptions => Unit): ConsoleOptions[Self] = {
    mods.put(name, ((args: Args) => if (args.boolean(name)) setter() else _ => ()))
    this
  }

  def addOptional(name: String, setter: String => SqoopOptions => Unit, default: Option[String] = None): ConsoleOptions[Self] = {
    addOptional(name, setter, v => v, default)
    this
  }

  def addOptional[T](name: String, setter: T => SqoopOptions => Unit, converter: String => T, default: Option[T] = None): ConsoleOptions[Self] = {
    val f = (args: Args) => {
      args.optional(name) match {
        case Some(value)                => setter(converter(value))
        case None if args.boolean(name) => {
          //hack-fix for empty string parameters, e.g. --input-null-string ""
          //scalding.Args treats "" as empty value, it's simply dropped

          //more technically: here we satisfy (args.optional(name) == None && args.boolean(name) != None)
          //it means args.m.get(name) == List(). Since it's not a boolean parameter we know there was an attempt to set
          //a parameter with some value, but the value is now missing
          setter(converter(""))
        }
        case None                       =>
          default match {
            case Some(defaultValue) => setter(defaultValue)
            case None               => s:SqoopOptions => ()
          }
      }
    }
    mods.put(name, f)
    this
  }

  def setOptions(args: Args): Self = {
    mods.foldLeft(this.asInstanceOf[Self]) {
      case (dsl, (argName, mod)) =>
        dsl.update(mod(args)).asInstanceOf[Self]
    }
  }

  def usage: String = {
    mods.map(_ match {
      case (name, setter) => s"--${name}"
    }).mkString("\n")
  }
}
