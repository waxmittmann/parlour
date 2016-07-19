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

import com.twitter.scalding.{Args, Execution, ExecutionApp}

import SqoopSyntax.{ParlourImportDsl, TeradataParlourImportDsl}

/**
  * A Basic Sqoop Job that can be invoked from the Console.
  *
  * Note - this job should only be used for testing/debugging purposes.
  * It specifically has bad password handling.
  */
object ImportSqoopConsoleJob extends ExecutionApp with SqoopConsoleJob {
  def job = for {
    args <- Execution.getConfig.map(_.getArgs)
    _    <- SqoopExecution.sqoopImport(optionsFromArgs(args))
  } yield ()

  /** Configures a Sqoop Job by parsing command line arguments */
  def optionsFromArgs(args: Args): ParlourImportOptions[_] = {
    val scheme = jdbcScheme(args("connection-string"))
    val dsl: ParlourImportOptions[_] = scheme match {
      case "teradata" => TeradataParlourImportDsl()
      case _          => ParlourImportDsl()
    }
    dsl.setOptions(args).asInstanceOf[ParlourImportOptions[_]]
  }
}
