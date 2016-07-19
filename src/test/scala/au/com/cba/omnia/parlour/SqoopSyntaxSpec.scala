package au.com.cba.omnia.parlour

import au.com.cba.omnia.parlour.SqoopSyntax._
import com.twitter.scalding.Args

class SqoopSyntaxSpec extends OmniaSpec { def is = s2"""
SqoopSyntax
===================

Should be able to apply console options:
  - with required arg               ${consoleOptions.requiredArg}
  - with boolean arg                ${consoleOptions.booleanArg}
  - with optional arg               ${consoleOptions.optionalArg}
  - with extraArgs-via-addBoolean   ${consoleOptions.extraArgsViaAddBoolean}
  - with extraArgs-via-addOptional  ${consoleOptions.extraArgsViaAddOptional}

Should be immutable:
  - without console options         ${immutable.withoutConsoleOpts}
  - with console options            ${immutable.withConsoleOpts}

Should offer getters:
  - for extra-value-args            ${getters.extraValueArgs}
  - for extra-boolean-args          ${getters.extraValueArgs}

Should set by default:
  - not skipDistCache               ${default.skipDistCache}
  - not be verbose by default       ${default.verbose}
  - connection manager for Teradata ${default.teradataConnManager}

Should preserve defaults when:
  - converting from Teradata Dsl    ${preserveDefaults.teradataConnManager}

Should build proper SqoopOptions:
  - for Teradata import             ${build.teradataImport}
  - for Teradata export             ${build.teradataExport}

"""
  object consoleOptions {
    def requiredArg = {
      //given
      val args = Args(List("--target-dir", "somedir"))
      //when
      val so = ParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getTargetDir must beEqualTo("somedir")
    }

    def booleanArg = {
      //given
      val args = Args("--verbose" :: requiredSqoopImportArgList)
      //when
      val so = ParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getVerbose must beEqualTo(true)
    }

    def optionalArg = {
      //given
      val args = Args("--teradata-batch-size" :: "7" :: requiredSqoopImportArgList)
      //when
      val so = TeradataParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getExtraArgs must beEqualTo(Array("--batch-size", "7"))
    }

    def extraArgsViaAddBoolean = {
      //given
      val args = Args("--teradata-skip-xview" :: requiredSqoopImportArgList)
      //when
      val so = TeradataParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getExtraArgs must beEqualTo(Array("--skip-xview"))
    }

    def extraArgsViaAddOptional = {
      //given
      val args = Args("--teradata-query-band" :: "someband" :: requiredSqoopImportArgList)
      //when
      val so = TeradataParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getExtraArgs must beEqualTo(Array("--query-band", "someband"))
    }
  }

  def requiredSqoopImportArgList() = List("--target-dir", "somedir")

  object immutable {
    def withoutConsoleOpts = {
      //given
      val parent = ParlourImportDsl()
        .username("username")

      //when
      val childOpts1 = parent.password("password 1").toSqoopOptions
      val childOpts2 = parent.password("password 2").toSqoopOptions

      //then
      childOpts1.getPassword must not be(childOpts2.getPassword)
    }

    def withConsoleOpts = {
      //given
      val parent = ParlourImportDsl().setOptions(Args(requiredSqoopImportArgList))

      //when
      val childOpts1 = parent.password("password 1").toSqoopOptions
      val childOpts2 = parent.password("password 2").toSqoopOptions

      //then
      childOpts1.getPassword must not be(childOpts2.getPassword)
    }
  }

  object getters {
    def extraValueArgs = {
      //given
      val someAdditionalValue = "hostname"
      val errorTable = "error-table"
      val dsl = TeradataParlourExportDsl().fastloadSocketHostName(someAdditionalValue).errorTable(errorTable)

      //when
      val res = dsl.getErrorTable

      //then
      res must beEqualTo(Some(errorTable))
    }

    def extraBooleanArgs = {
      //given
      val someAdditionalValue = 5
      val dsl = TeradataParlourImportDsl().numPartitionsForStagingTable(someAdditionalValue).keepStagingTable()

      //when
      val res = dsl.getKeepStagingTable

      //then
      res must beEqualTo(Some(true))
    }
  }

  object default {
    def skipDistCache = {
      val opts = ParlourImportDsl().toSqoopOptions
      opts.isSkipDistCache must beEqualTo(false)
    }

    def verbose = {
      val opts = ParlourImportDsl().toSqoopOptions
      opts.getVerbose must beEqualTo(false)
    }

    def teradataConnManager = {
      val opts = TeradataParlourImportDsl().toSqoopOptions
      opts.getConnManagerClassName must beEqualTo("com.cloudera.connector.teradata.TeradataManager")
    }
  }

  object preserveDefaults {
    def teradataConnManager = {
      val opts = ParlourImportDsl(TeradataParlourImportDsl().updates).toSqoopOptions
      opts.getConnManagerClassName must beEqualTo("com.cloudera.connector.teradata.TeradataManager")
    }
  }

  object build {
    def teradataImport = {
      //given
      val connString = "jdbc:teradata://some.server/database=DB1"
      val username = "some username"
      val password = "some password"
      val tableName = "some table"

      //when
      val sqoopOpts = TeradataParlourImportDsl()
        .inputMethod(SplitByAmp)
        .connectionString(connString)
        .username(username)
        .password(password)
        .tableName(tableName)
        .toSqoopOptions

      //then
      sqoopOpts.getExtraArgs must beEqualTo(Array("--input-method", "split.by.amp"))
      sqoopOpts.getConnectString must beEqualTo(connString)
      sqoopOpts.getUsername must beEqualTo(username)
      sqoopOpts.getPassword must beEqualTo(password)
      sqoopOpts.getTableName must beEqualTo(tableName)
    }

    def teradataExport = {
      //given
      val connString = "jdbc:teradata://some.server/database=DB1"
      val username = "some username"
      val password = "some password"
      val tableName = "some table"
      val errorDb = "errorDB"

      //when
      val sqoopOpts = TeradataParlourExportDsl()
        .outputMethod(BatchInsert)
        .connectionString(connString)
        .username(username)
        .password(password)
        .tableName(tableName)
        .errorDatabase(errorDb)
        .toSqoopOptions

      //then
      sqoopOpts.getExtraArgs must beEqualTo(Array("--output-method", "batch.insert", "--error-database", "errorDB"))
      sqoopOpts.getConnectString must beEqualTo(connString)
      sqoopOpts.getUsername must beEqualTo(username)
      sqoopOpts.getPassword must beEqualTo(password)
      sqoopOpts.getTableName must beEqualTo(tableName)
    }
  }
}
