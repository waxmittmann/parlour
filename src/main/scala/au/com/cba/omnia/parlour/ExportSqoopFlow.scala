package au.com.cba.omnia.parlour

import java.util.{Collection => JCollection}

import scala.collection.JavaConverters._

import cascading.flow.hadoop.ProcessFlow
import cascading.scheme.hadoop.TextDelimited
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs

import com.cloudera.sqoop.SqoopOptions

import org.apache.sqoop.tool.ExportTool

import riffle.process._

import scalaz._, Scalaz._

/** Implements a Cascading Flow that wraps the Sqoop Process */
class ExportSqoopFlow(name: String,
                      options: SqoopOptions,
                      source: Tap[_, _, _],
                      sink: Tap[_, _, _])
  extends ProcessFlow[ExportSqoopRiffle](name, new ExportSqoopRiffle(options, source, sink)) {
}

object ExportSqoopRiffle {
  private def getSingleCharacter(name: String, value: Option[String]): String \/ Option[Char] =
    if (value.map(_.length == 1).getOrElse(true)) value.map(_.head).right
    else s"$name had multiple characters for a delimiter - this is not supported by Sqoop".left

  def setDelimitersFromTap(source: Tap[_, _, _], options: SqoopOptions) =
    source.getScheme() match {
      case delimited: TextDelimited => for {
        quote     <- getSingleCharacter("Quote", Option(delimited.getQuote))
        delimiter <- getSingleCharacter("Delimiter", Option(delimited.getDelimiter))
      } yield {
        quote.foreach(options.setEscapedBy)
        delimiter.foreach(options.setInputFieldsTerminatedBy)
        options.setInputLinesTerminatedBy('\n')
      }
      case scheme => s"Unknown scheme used by tap: $scheme (${scheme.getClass.getName})".left
    }

  def setPathFromTap(source: Tap[_, _, _], options: SqoopOptions) =
    source match {
      case hfs: Hfs => options.setExportDir(hfs.getPath.toString).right
      case tap      => s"Unknown tap used: $tap (${tap.getClass.getName})".left
    }
}

/** Implements a Riffle for a Sqoop Job */
@Process
class ExportSqoopRiffle(options: SqoopOptions,
                        source: Tap[_, _, _],
                        sink: Tap[_, _, _],
                        inferPathFromTap: Boolean = true,
                        inferSourceDelimitersFromTap: Boolean = true) {

  @ProcessStart
  def start(): Unit = ()

  @ProcessStop
  def stop(): Unit = ()
  
  @ProcessComplete
  def complete(): Unit = {
    // Extract the source path from the source tap.
    if (inferPathFromTap) {
      ExportSqoopRiffle.setPathFromTap(source, options)
        .leftMap({ error => println(s"Couldn't infer path from source tap.\n\t$error") })
    }

    // Extract the delimiters from the source tap.
    if (inferSourceDelimitersFromTap) {
      ExportSqoopRiffle.setDelimitersFromTap(source, options)
        .leftMap({ error => println(s"Couldn't infer delimiters from source tap's scheme.\n\t$error") })
    }

    // Required because the Code Generation fails when run via Scalding
    // (the fat jar isn't on the classpath and thus neither is Sqoop).
    // NOTE: this will only work for exporting to Teradata.
    options.setExistingJarName(getClass.getProtectionDomain().getCodeSource().getLocation().getPath())
    new ExportTool().run(options)
  }

  @DependencyIncoming
  def getIncoming(): JCollection[_] = List(source).asJava

  @DependencyOutgoing
  def getOutgoing(): JCollection[_] = List(sink).asJava
}
