package au.com.cba.omnia.parlour

import au.com.cba.omnia.parlour.SqoopSyntax._
import com.twitter.scalding.Args
import org.specs2.mutable.Specification

class SqoopSyntaxSpec extends Specification {
  "Console Options" should {
    " create Parlour DSL with required arg" in {
      //given
      val args = Args(List("--target-dir", "somedir"))
      //when
      val so = ParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getTargetDir must beEqualTo("somedir")
    }
    " create Parlour DSL with boolean arg" in {
      //given
      val args = Args("--verbose" :: requiredSqoopImportArgList)
      //when
      val so = ParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getVerbose must beEqualTo(true)
    }
    " create Parlour DSL with optional arg" in {
      //given
      val args = Args("--teradata-batch-size" :: "7" :: requiredSqoopImportArgList)
      //when
      val so = TeradataParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getExtraArgs must beEqualTo(Array("--batch-size", "7"))
    }
    " create Parlour DSL with extraArgs-via-addBoolean arg" in {
      //given
      val args = Args("--teradata-skip-xview" :: requiredSqoopImportArgList)
      //when
      val so = TeradataParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getExtraArgs must beEqualTo(Array("--skip-xview"))
    }
    " create Parlour DSL with extraArgs-via-addOptional arg" in {
      //given
      val args = Args("--teradata-query-band" :: "someband" :: requiredSqoopImportArgList)
      //when
      val so = TeradataParlourImportDsl().setOptions(args).toSqoopOptions
      //then
      so.getExtraArgs must beEqualTo(Array("--query-band", "someband"))
    }
  }

  def requiredSqoopImportArgList() = List("--target-dir", "somedir")

  "Parlour DSL" should {
    " be immutable without console args" in {
      //given
      val parent = ParlourImportDsl()
        .username("username")

      //when
      val childOpts1 = parent.password("password 1").toSqoopOptions
      val childOpts2 = parent.password("password 2").toSqoopOptions

      //then
      childOpts1.getPassword must not be(childOpts2.getPassword)
    }

    " be immutable with console args" in {
    //given
    val parent = ParlourImportDsl().setOptions(Args(requiredSqoopImportArgList))

    //when
    val childOpts1 = parent.password("password 1").toSqoopOptions
    val childOpts2 = parent.password("password 2").toSqoopOptions

    //then
    childOpts1.getPassword must not be(childOpts2.getPassword)
    }
    " allow to get extra-value-args" in {
      //given
      val someAdditionalValue = "hostname"
      val errorTable = "error-table"
      val dsl = TeradataParlourExportDsl().fastloadSocketHostName(someAdditionalValue).errorTable(errorTable)

      //when
      val res = dsl.getErrorTable

      //then
      res must beEqualTo(Some(errorTable))
    }
    " allow to get extra-boolean-args" in {
      //given
      val someAdditionalValue = 5
      val dsl = TeradataParlourImportDsl().numPartitionsForStagingTable(someAdditionalValue).keepStagingTable()

      //when
      val res = dsl.getKeepStagingTable

      //then
      res must beEqualTo(Some(true))
    }
  }

  "SqoopOptions for all Dsls" should {
    " not skipDistCache by default" in {
      val opts = ParlourImportDsl().toSqoopOptions
      opts.isSkipDistCache must beEqualTo(false)
    }

    " not be verbose by default" in {
      val opts = ParlourImportDsl().toSqoopOptions
      opts.getVerbose must beEqualTo(false)
    }
  }

  "SqoopOptions for all TeradataParlourDsls" should {
    " have connManager by default" in {
      val opts = TeradataParlourImportDsl().toSqoopOptions
      opts.getConnManagerClassName must beEqualTo("com.cloudera.connector.teradata.TeradataManager")
    }
  }

  "TeradataParlourImportDsl" should {
    " create proper SqoopOptions for simple example" in {
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

    "TeradataParlourExportDsl" should {
      " create proper SqoopOptions for simple example" in {
        //given
        val connString = "jdbc:teradata://some.server/database=DB1"
        val username = "some username"
        val password = "some password"
        val tableName = "some table"

        //when
        val sqoopOpts = TeradataParlourExportDsl()
          .outputMethod(BatchInsert)
          .connectionString(connString)
          .username(username)
          .password(password)
          .tableName(tableName)
          .toSqoopOptions

        //then
        sqoopOpts.getExtraArgs must beEqualTo(Array("--output-method", "batch.insert"))
        sqoopOpts.getConnectString must beEqualTo(connString)
        sqoopOpts.getUsername must beEqualTo(username)
        sqoopOpts.getPassword must beEqualTo(password)
        sqoopOpts.getTableName must beEqualTo(tableName)
      }
    }
  }
}
