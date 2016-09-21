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

import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.scheme.hadoop.TextDelimited

/** Infers settings from source/sink taps. */
object SqoopSetup {
  case class Delimiters(quote: Option[Char], fieldDelimiter: Option[Char])

  /** Infers HFS path from tap. */
  def inferPathFromTap(tap: Tap[_, _, _]): Option[String] =
    tap match {
      case hfs: Hfs => Some(hfs.getPath.toString)
      case tap      => {
        println(s"Couldn't infer path from tap.\n\tUnknown tap used: $tap (${tap.getClass.getName})")
        None
      }
    }

  /** Infers quote and field delimeter symbols from tap. */
  def inferDelimitersFromTap(tap: Tap[_, _, _]): Delimiters =
    tap.getScheme match {
      case delimited: TextDelimited => {
        val quote     = getSingleCharacter("Quote", Option(delimited.getQuote))
        val delimiter = getSingleCharacter("Delimiter", Option(delimited.getDelimiter))

        Delimiters(quote, delimiter)
      }
      case scheme => {
        println(s"Couldn't infer delimiters from tap's scheme.\n\tUnknown scheme used by tap: $scheme (${scheme.getClass.getName})")
        Delimiters(None, None)
      }
  }

  private def getSingleCharacter(name: String, value: Option[String]): Option[Char] =
    if (value.map(_.length == 1).getOrElse(true)) value.map(_.head)
    else {
      println(s"$name had multiple characters for a delimiter - this is not supported by Sqoop")
      None
    }
}
