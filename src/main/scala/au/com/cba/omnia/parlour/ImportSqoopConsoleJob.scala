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

import com.cloudera.sqoop.SqoopOptions
import com.twitter.scalding.{Args, NullTap}

import SqoopSyntax.{ParlourImportDsl, TeradataParlourImportDsl}

object ImportSqoopConsoleJob extends SqoopConsoleJob {
  /** Configures a Sqoop Job by parsing command line arguments */
  def optionsFromArgs(args: Args): SqoopOptions = {
    import SqoopSyntax._
    val scheme = jdbcScheme(args("connection-string"))
    val dsl = scheme match {
      case "teradata" => TeradataParlourImportDsl()
      case _          => ParlourImportDsl()
    }
    dsl.consoleArguments.setOptions(args).toSqoopOptions
  }
}

/**
 * A Basic Sqoop Job that can be invoked from the Console.
 *
 * Note - this job should only be used for testing/debugging purposes.
 * It specifically has bad password handling.
 */
class ImportSqoopConsoleJob(args: Args)
  extends ImportSqoopJob(ImportSqoopConsoleJob.optionsFromArgs(args), new NullTap(), new NullTap())(args)

