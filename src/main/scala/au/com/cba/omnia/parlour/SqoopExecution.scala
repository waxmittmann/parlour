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

import java.util.UUID

import scalaz.Scalaz._

import org.apache.hadoop.conf.Configuration

import com.twitter.scalding.{Args, Execution, Mode, Read, Source, Write}

import org.apache.sqoop.Sqoop
import org.apache.sqoop.tool.{EvalSqlTool, ExportTool, ImportTool}

import au.com.cba.omnia.parlour.SqoopSyntax.{ParlourExportDsl, ParlourImportDsl}
import au.com.cba.omnia.parlour.SqoopSetup.Delimiters

/** Factory for Sqoop Executions */
object SqoopExecution {
  /**
    * An Execution that uses sqoop to import data to a tap.
    *
    * `options` does not need any information about the destination.
    * We infer that information from `sink`.
    */
  def sqoopImport(options: ParlourImportOptions[_], sink: Source): Execution[Unit] = for {
    mode                         <- Execution.getMode
    tap                           = sink.createTap(Write)(mode)
    sourcePath                    = SqoopSetup.inferPathFromTap(tap)
    Delimiters(quote, fieldDelim) = SqoopSetup.inferDelimitersFromTap(tap)
    dsl                           = ParlourImportDsl(options.updates)
    withTargetDir                 = sourcePath.cata(dsl.targetDir, dsl)
    withQuote                     = quote.cata(withTargetDir.escapedBy, withTargetDir)
    withDelim                     = fieldDelim.cata(withQuote.fieldsTerminatedBy, withQuote)
    withTerminator                = withDelim.linesTerminatedBy('\n')
    _                            <- sqoopImport(withTerminator)
  } yield ()

  /** An Execution that uses sqoop to import data. */
  def sqoopImport(options: ParlourImportOptions[_]): Execution[Unit] = Execution.from {
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true")
    val dsl = ParlourImportDsl(options.updates)
    new ImportTool().run(dsl.toSqoopOptions)
  }

  /**
    * An Execution that uses sqoop to export data from a tap to a database.
    *
    * `options` does not need any information about the source.
    * We infer that information from `source`.
    */
  def sqoopExport(options: ParlourExportOptions[_], source: Source): Execution[Unit] = for {
    mode                         <- Execution.getMode
    tap                           = source.createTap(Read)(mode)
    sourcePath                    = SqoopSetup.inferPathFromTap(tap)
    Delimiters(quote, fieldDelim) = SqoopSetup.inferDelimitersFromTap(tap)
    dsl                           = ParlourExportDsl(options.updates)
    withExportDir                 = sourcePath.cata(dsl.exportDir, dsl)
    withQuote                     = quote.cata(withExportDir.inputEscapedBy, withExportDir)
    withDelim                     = fieldDelim.cata(withQuote.inputFieldsTerminatedBy, withQuote)
    withTerminator                = withDelim.inputLinesTerminatedBy('\n')
    _                            <- sqoopExport(withTerminator)
  } yield ()

  /** An Execution that uses sqoop to export data to a database. */
  def sqoopExport(options: ParlourExportOptions[_]): Execution[Unit] = Execution.from {
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true")
    val dsl = ParlourExportDsl(options.updates)

    SqoopEval.evalSql(dsl)
    new ExportTool().run(dsl.toSqoopOptions)
  }
}
