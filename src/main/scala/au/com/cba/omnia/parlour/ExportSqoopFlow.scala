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
 * Appends data to the target table.
 */
class ExportSqoopFlow(
  name: String,
  options: ParlourExportOptions[_],
  source: Option[Tap[_, _, _]],
  sink: Option[Tap[_, _, _]],
  inferPathFromTap: Boolean = true,
  inferSourceDelimitersFromTap: Boolean = true
) extends SqoopFlow(name, source, sink)(ExportSqoop.doExport(options, inferPathFromTap, inferSourceDelimitersFromTap))

/**
 * Implements a Cascading Flow that wraps the Sqoop Delete and Export Process.
 * Deletes all rows from the target table before export.
 */
class DeleteAndExportSqoopFlow(
  name: String,
  options: ParlourExportOptions[_],
  source: Option[Tap[_, _, _]],
  sink: Option[Tap[_, _, _]],
  inferPathFromTap: Boolean = true,
  inferSourceDelimitersFromTap: Boolean = true
) extends SqoopFlow(name, source, sink)(ExportSqoop.doDeleteAndExport(options, inferPathFromTap, inferSourceDelimitersFromTap))

/**
 * Logic for Sqoop Export with appending data or deleting data first.
 */
object ExportSqoop {
  def doExport(options: ParlourExportOptions[_], inferPathFromTap: Boolean, inferSourceDelimitersFromTap: Boolean)
            (source: Option[Tap[_, _, _]], sink: Option[Tap[_, _, _]]): Unit = {
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true")

    val dsl = ParlourExportDsl(options.updates)
    val inferredOptions = inferFromSourceTap(dsl, source, inferPathFromTap, inferSourceDelimitersFromTap)

    new ExportTool().run(inferredOptions.toSqoopOptions)
  }

  def doDeleteAndExport(options: ParlourExportOptions[_], inferPathFromTap: Boolean, inferSourceDelimitersFromTap: Boolean)
                     (source: Option[Tap[_, _, _]], sink: Option[Tap[_, _, _]]): Unit = {
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true")

    val dsl = ParlourExportDsl(options.updates)
    delete(dsl)
    val inferredDsl = inferFromSourceTap(dsl, source, inferPathFromTap, inferSourceDelimitersFromTap)

    new ExportTool().run(inferredDsl.toSqoopOptions)
  }

  private def delete(dsl: ParlourExportDsl) = {
    val withDeleteQuery = dsl sqlQuery s"DELETE FROM ${dsl.getTableName.get}"
    new EvalSqlTool().run(withDeleteQuery.toSqoopOptions)
  }

  private def inferFromSourceTap(dsl: ParlourExportDsl, source: Option[Tap[_, _, _]],
                         inferPathFromTap: Boolean, inferSourceDelimitersFromTap: Boolean): ParlourExportDsl = {
    val sourcePathOpt = SqoopSetup.inferPathFromTap(inferPathFromTap, source)
    val sourceDelimiters = SqoopSetup.inferDelimitersFromTap(inferSourceDelimitersFromTap, source)

    val withExportDir = sourcePathOpt.fold(dsl)(dsl exportDir _)

    val withDelimiters = sourceDelimiters match {
      case Delimiters(quoteOpt, fieldDelimOpt) =>
        val withQuote = quoteOpt.fold(withExportDir)(withExportDir inputEscapedBy _)
        val withDelim = fieldDelimOpt.fold(withQuote)(withQuote inputFieldsTerminatedBy _)

        if (inferSourceDelimitersFromTap)
          withDelim inputLinesTerminatedBy '\n'
        else withDelim
    }

    withDelimiters
  }
}