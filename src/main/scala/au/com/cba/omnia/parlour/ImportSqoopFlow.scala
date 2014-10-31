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

import org.apache.sqoop.tool.ImportTool

import scalaz.Scalaz._
import scalaz.{\/ => \/}

import cascading.flow.hadoop.ProcessFlow
import cascading.scheme.hadoop.TextDelimited
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs

import riffle.process._

/** Implements a Cascading Flow that wraps the Sqoop Process */
class ImportSqoopFlow(name: String,
  options: SqoopOptions,
  source: Tap[_, _, _],
  sink: Tap[_, _, _]
) extends FixedProcessFlow[ImportSqoopRiffle](name, new ImportSqoopRiffle(options, source, sink)) {
}

object ImportSqoopRiffle {
  private def getSingleCharacter(name: String, value: Option[String]): String \/ Option[Char] = {
    if (value.cata(_.length == 1, true)) value.map(_.head).right
    else s"$name had multiple characters for a delimiter, got \${value.getOrElse("")} - this is not supported by Sqoop".left
  }

  def setDelimitersFromTap(sink: Tap[_, _, _], options: SqoopOptions) =
    sink.getScheme() match {
      case delimited: TextDelimited => for {
        quote <- getSingleCharacter("Quote", Option(delimited.getQuote))
        delimiter <- getSingleCharacter("Delimiter", Option(delimited.getDelimiter))
      } yield {
        quote.foreach(options.setEscapedBy)
        delimiter.foreach(options.setFieldsTerminatedBy)
        options.setLinesTerminatedBy('\n')
      }
      case scheme => s"Unknown scheme used by tap: $scheme (${scheme.getClass.getName})".left
    }

  def setTargetPathFromTap(tap: Tap[_, _, _], options: SqoopOptions) =
    tap match {
      case hfs: Hfs => options.setTargetDir(hfs.getPath.toString).right
      case tap      => s"Unknown tap used: $tap (${tap.getClass.getName})".left
    }
}

/** Implements a Riffle for a Sqoop Job */
@Process
class ImportSqoopRiffle(options: SqoopOptions,
  source: Tap[_, _, _],
  sink: Tap[_, _, _],
  inferPathFromTap: Boolean = true,
  inferSinkDelimitersFromTap: Boolean = true) {

  @ProcessStart
  def start(): Unit = ()

  @ProcessStop
  def stop(): Unit = ()

  @ProcessComplete
  def complete(): Unit = {
    // Extract the target path from the sink tap.
    if (inferPathFromTap) {
      ImportSqoopRiffle.setTargetPathFromTap(sink, options)
        .leftMap({ error => println(s"Couldn't infer path from sink tap.\n\t$error") })
    }

    // Extract the delimiters from the source tap.
    if (inferSinkDelimitersFromTap) {
      ImportSqoopRiffle.setDelimitersFromTap(sink, options)
        .leftMap({ error => println(s"Couldn't infer delimiters from sink tap's scheme.\n\t$error") })
    }

    new ImportTool().run(options)
  }

  @DependencyIncoming
  def getIncoming(): JCollection[_] = List(source).asJava

  @DependencyOutgoing
  def getOutgoing(): JCollection[_] = List(sink).asJava
}
