package au.com.cba.omnia.parlour

import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.scheme.hadoop.TextDelimited

/**
 * Infers settings from source/sink taps.
 */
object SqoopSetup {
  case class Delimiters(quote: Option[Char], fieldDelimiter: Option[Char])

  def inferPathFromTap(infer: Boolean, tapOpt: Option[Tap[_, _, _]]): Option[String] = {
    if (infer) {
      tapOpt.flatMap {
        case hfs: Hfs => Some(hfs.getPath.toString)
        case tap      => {
          println(s"Couldn't infer path from tap.\n\tUnknown tap used: $tap (${tap.getClass.getName})")
          None
        }
      }
    } else {
      None
    }
  }

  def inferDelimitersFromTap(infer: Boolean, tapOpt: Option[Tap[_, _, _]]): Delimiters = {
    if (infer) {
      tapOpt.map(_.getScheme).map {
        case delimited: TextDelimited => {
          val quote     = getSingleCharacter("Quote", Option(delimited.getQuote))
          val delimiter = getSingleCharacter("Delimiter", Option(delimited.getDelimiter))

          Delimiters(quote, delimiter)
        }
        case scheme => {
          println(s"Couldn't infer delimiters from tap's scheme.\n\tUnknown scheme used by tap: $scheme (${scheme.getClass.getName})")
          Delimiters(None, None)
        }
      } getOrElse(Delimiters(None, None))
    } else {
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
