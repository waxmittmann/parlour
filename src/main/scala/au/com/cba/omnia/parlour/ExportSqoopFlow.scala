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

import org.apache.sqoop.Sqoop
import org.apache.sqoop.tool.{EvalSqlTool, ExportTool}

import cascading.tap.Tap

import au.com.cba.omnia.parlour.flow.SqoopFlow
import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl
import au.com.cba.omnia.parlour.SqoopSetup.Delimiters

/**
  * Implements a Cascading Flow that wraps the Sqoop Export Process.
  * If a SQL query is set on 'options' param, then it is executed before export.
 */
class ExportSqoopFlow(
  name: String,
  options: ParlourExportOptions[_],
  source: Option[Tap[_, _, _]],
  sink: Option[Tap[_, _, _]],
  inferPathFromSourceTap: Boolean = true,
  inferDelimitersFromSourceTap: Boolean = true
) extends SqoopFlow(name, source, sink)(ExportSqoop.doExport(options, inferPathFromSourceTap, inferDelimitersFromSourceTap))

/** Logic for Sqoop Export with appending data or deleting data first. */
object ExportSqoop {
  def doExport
    (options: ParlourExportOptions[_], inferPathFromSourceTap: Boolean, inferDelimitersFromSourceTap: Boolean)
    (source: Option[Tap[_, _, _]], sink: Option[Tap[_, _, _]]): Unit = {
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true")

    val dsl = ParlourExportDsl(options.updates)
    val inferredOptions = inferFromSourceTap(dsl, source, inferPathFromSourceTap, inferDelimitersFromSourceTap)

    SqoopEval.evalSql(dsl)
    new ExportTool().run(inferredOptions.toSqoopOptions)
  }

  private def inferFromSourceTap(
    dsl: ParlourExportDsl, source: Option[Tap[_, _, _]],
    inferPathFromSourceTap: Boolean, inferDelimitersFromSourceTap: Boolean
  ): ParlourExportDsl = {
    val sourcePathOpt = SqoopSetup.inferPathFromTap(inferPathFromSourceTap, source)
    val sourceDelimiters = SqoopSetup.inferDelimitersFromTap(inferDelimitersFromSourceTap, source)

    val withExportDir = sourcePathOpt.fold(dsl)(dsl exportDir _)

    val withDelimiters = sourceDelimiters match {
      case Delimiters(quoteOpt, fieldDelimOpt) =>
        val withQuote = quoteOpt.fold(withExportDir)(withExportDir inputEscapedBy _)
        val withDelim = fieldDelimOpt.fold(withQuote)(withQuote inputFieldsTerminatedBy _)

        if (inferDelimitersFromSourceTap)
          withDelim inputLinesTerminatedBy '\n'
        else withDelim
    }

    withDelimiters
  }
}
