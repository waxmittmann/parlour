package au.com.cba.omnia.parlour

import com.cloudera.sqoop.SqoopOptions

/**
 * Provides a DSL for creating Sqoop Options
 *
 * See `README.md` for an example of usage.
 */
object SqoopSyntax {
  def sqoopOptions(): SqoopOptions = new SqoopOptions()

  sealed trait TeradataMethod
  case object BatchInsert extends TeradataMethod
  case object InternalFastload extends TeradataMethod
  case object MultipleFastload extends TeradataMethod

  implicit class SqoopDsl(options: SqoopOptions) {
    private def update(f: SqoopOptions => Unit): SqoopOptions = {
      f(options)
      options
    }

    def teradata(method: TeradataMethod): SqoopOptions = {
      TeradataSqoopOptions.useTeradataDriver(options)
      TeradataSqoopOptions.exportMethod(options, method match {
        case BatchInsert      => "batch.insert"
        case InternalFastload => "internal.fastload"
        case MultipleFastload => "multiple.fastload"
      })
      options
    }

    def inputDirectory(directory: String) =
      update(_.setExportDir(directory))

    def connectionString(connectionString: String) =
      update(_.setConnectString(connectionString))

    def username(username: String) =
      update(_.setUsername(username))

    def password(password: String) =
      update(_.setPassword(password))

    def tableName(tableName: String) =
      update(_.setTableName(tableName))

    def numberOfMappers(count: Int) =
      update(_.setNumMappers(count))

    def fieldDelimiter(delimiter: Char) =
      update(_.setInputFieldsTerminatedBy(delimiter))

    def lineDelimiter(delimiter: Char) =
      update(_.setInputLinesTerminatedBy(delimiter))
  }
}

/** Options specific to the Teradata Connector */
object TeradataSqoopOptions {
  def useTeradataDriver(options: SqoopOptions): SqoopOptions = {
    options.setConnManagerClassName("com.cloudera.connector.teradata.TeradataManager")
    options
  }

  def exportMethod(options: SqoopOptions, method: String) = {
    options.setExtraArgs(Array("--output-method", method))
    options
  }
}


