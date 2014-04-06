package au.com.cba.omnia.parlour

import com.twitter.scalding._, Dsl._, TDsl._

import org.apache.hadoop.conf.Configuration

import SqoopSyntax._

class ExportSqoopRiffleSpec extends OmniaSpec { def is = s2"""
Export Sqoop Riffle
===================

Should be able to infer the path for:
  - TypedPsv ${inferPath.typedPsv}
  - Csv      ${inferPath.csv}

Should be able to infer delimiters for:
  - TypedPsv  ${inferDelimiter.typedPsv}
  - Csv       ${inferDelimiter.csv}

"""

  object inferPath {
    def typedPsv = {
      val options = sqoopOptions()
      ExportSqoopRiffle.setPathFromTap(
        TypedPsv[String]("/test/path").createTap(Read)(Hdfs(false, new Configuration())),
        options
      ).toEither must beRight
      options.getExportDir must beEqualTo("/test/path")
    }

    def csv = {
      val options = sqoopOptions()
      ExportSqoopRiffle.setPathFromTap(
        Csv("/test/path").createTap(Read)(Hdfs(false, new Configuration())),
        options
      ).toEither must beRight
      options.getExportDir must beEqualTo("/test/path")
    }
  }

  object inferDelimiter {
    def typedPsv = {
      val options = sqoopOptions()
      ExportSqoopRiffle.setDelimitersFromTap(
        TypedPsv[String]("test").createTap(Read)(Hdfs(false, new Configuration())),
        options
      ).toEither must beRight
      options.getInputFieldDelim must beEqualTo('|')
    }

    def csv = {
      val options = sqoopOptions()
      ExportSqoopRiffle.setDelimitersFromTap(
        Csv("test").createTap(Read)(Hdfs(false, new Configuration())),
        options
      ).toEither must beRight
      options.getInputFieldDelim must beEqualTo(',')
    }
  }
}
