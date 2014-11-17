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
import org.apache.sqoop.tool.ImportTool

import cascading.tap.Tap

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl
import au.com.cba.omnia.parlour.flow.SqoopFlow
import au.com.cba.omnia.parlour.SqoopSetup.Delimiters

/** Implements a Cascading Flow that wraps the Sqoop Import Process */
class ImportSqoopFlow(
  name: String,
  options: ParlourImportOptions[_],
  source: Option[Tap[_, _, _]],
  sink: Option[Tap[_, _, _]],
  inferTargetPathFromTap: Boolean = true,
  inferSinkDelimitersFromTap: Boolean = true
) extends SqoopFlow(name, source, sink)(ImportSqoop.doImport(options, inferTargetPathFromTap, inferSinkDelimitersFromTap))

/**
 * Logic for Sqoop Import with appending or deleting data first.
 */
object ImportSqoop {
  def doImport(options: ParlourImportOptions[_], inferTargetPathFromTap: Boolean, inferSinkDelimitersFromTap: Boolean)
              (source: Option[Tap[_, _, _]], sink: Option[Tap[_, _, _]]): Unit = {
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true")

    val dsl = ParlourImportDsl(options.updates)
    val inferredDsl = inferFromSinkTap(dsl, sink, inferTargetPathFromTap, inferSinkDelimitersFromTap)

    new ImportTool().run(inferredDsl.toSqoopOptions)
  }

  private def inferFromSinkTap(dsl: ParlourImportDsl, sink: Option[Tap[_, _, _]],
                       inferTargetPathFromTap: Boolean, inferSinkDelimitersFromTap: Boolean): ParlourImportDsl = {
    val targetPathOpt = SqoopSetup.inferPathFromTap(inferTargetPathFromTap, sink)
    val sinkDelimiters = SqoopSetup.inferDelimitersFromTap(inferSinkDelimitersFromTap, sink)

    val withTargetDir = targetPathOpt.fold(dsl)(dsl targetDir _)

    val withDelimiters = sinkDelimiters match {
      case Delimiters(quoteOpt, fieldDelimOpt) =>
        val withQuote = quoteOpt.fold(withTargetDir)(withTargetDir escapedBy _)
        val withDelim = fieldDelimOpt.fold(withQuote)(withQuote fieldsTerminatedBy _)

        if (inferSinkDelimitersFromTap)
          withDelim linesTerminatedBy '\n'
        else withDelim
    }

    withDelimiters
  }
}
