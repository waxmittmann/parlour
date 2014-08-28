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

import scala.collection.JavaConverters.seqAsJavaListConverter

import java.util.{Collection => JCollection}

import com.cloudera.sqoop.SqoopOptions

import org.apache.sqoop.tool.ExportTool

import scalaz.Scalaz._
import scalaz.{\/ => \/}

import cascading.flow.hadoop.ProcessFlow
import cascading.scheme.hadoop.TextDelimited
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs

import riffle.process._

/** Implements a Cascading Flow that wraps the Sqoop Process */
class ExportSqoopFlow(
  name: String,
  options: SqoopOptions,
  source: Tap[_, _, _],
  sink: Tap[_, _, _]
) extends ProcessFlow[ExportSqoopRiffle](name, new ExportSqoopRiffle(options, source, sink)) {
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

    new ExportTool().run(options)
  }

  @DependencyIncoming
  def getIncoming(): JCollection[_] = List(source).asJava

  @DependencyOutgoing
  def getOutgoing(): JCollection[_] = List(sink).asJava
}
