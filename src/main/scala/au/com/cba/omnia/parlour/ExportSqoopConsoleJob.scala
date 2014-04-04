package au.com.cba.omnia.parlour

import com.cloudera.sqoop.SqoopOptions

import com.twitter.scalding._

object ExportSqoopConsoleJob {
  /** Configures a Sqoop Job by parsing command line arguments */
  def optionsFromArgs(args: Args): SqoopOptions = {
    val options = new SqoopOptions()

    options.setExportDir(args("input")) // directory on HDFS to export to Teradata
    args.optional("connection-string").foreach(options.setConnectString)
    args.optional("username").foreach(options.setUsername)
    args.optional("password").foreach(options.setPassword)
    args.optional("table-name").foreach(options.setTableName)
    args.optional("mappers").map(_.toInt).foreach(options.setNumMappers)
    options.setInputFieldsTerminatedBy(args.getOrElse("input-field-delimiter", "|").head)
    options.setInputLinesTerminatedBy(args.getOrElse("input-line-delimiter", "\n").head)

    args.optional("teradata-method").foreach(method =>
      TeradataSqoopOptions.exportMethod(options, method)
    )

    if (args.boolean("teradata")) TeradataSqoopOptions.useTeradataDriver(options)

    options
  }
}

/**
 * A Basic Sqoop Job that can be invoked from the Console.
 *
 * Note - this job should only be used for testing/debugging purposes.
 * It specifically has bad password handling.
 */
class ExportSqoopConsoleJob(args: Args)
  extends ExportSqoopJob(ExportSqoopConsoleJob.optionsFromArgs(args), new NullTap())(args)