package com.technext.demos.txbuffering

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.mutable

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import com.technext.demos.txbuffering.model.spec.TestModels

class CdcModelToolTest {

  private def run(args: String*): (Int, String, String) = {
    val out = mutable.ArrayBuffer[String]()
    val err = mutable.ArrayBuffer[String]()
    val code = CdcModelTool.run(args, out += _, err += _)
    (code, out.mkString("\n"), err.mkString("\n"))
  }

  private def read(p: java.nio.file.Path): String = new String(Files.readAllBytes(p), StandardCharsets.UTF_8)

  @Test
  def ddlForTheOrdersJobIsWhatIsCheckedIn(): Unit = {
    val dir = Files.createTempDirectory("ddl")
    val (code, out, _) = run("ddl", "jobs/orders.conf", dir.toString)
    assertThat(code).isEqualTo(0)
    assertThat(out).contains("orders_stream.deltalake.sql")
    assertThat(out).contains("orders_current.deltalake.sql")
    Seq("orders_stream", "orders_current").foreach { f =>
      assertThat(read(dir.resolve(s"$f.deltalake.sql"))).isEqualTo(read(Paths.get(s"db/$f.deltalake.sql")))
    }
  }

  @Test
  def writingTwiceGivesTheSameFiles(): Unit = {
    val dir = Files.createTempDirectory("ddl")
    run("ddl", "jobs/orders.conf", dir.toString)
    val first = read(dir.resolve("orders_current.deltalake.sql"))
    run("ddl", "jobs/orders.conf", dir.toString)
    assertThat(read(dir.resolve("orders_current.deltalake.sql"))).isEqualTo(first)
  }

  @Test
  def checkReportsEveryProblem(): Unit = {
    val f = Files.createTempFile("bad", ".conf")
    Files.writeString(f, TestModels.Compound.replace("cardinality = many", "").replace("type = int", "type = innt"))
    val (code, _, err) = run("check", f.toString)
    assertThat(code).isEqualTo(1)
    assertThat(err).contains("shipments: child has no cardinality")
    assertThat(err).contains("'innt' is not a valid Spark SQL type")
  }

  @Test
  def checkAcceptsAValidJob(): Unit = {
    val (code, out, _) = run("check", "jobs/orders.conf")
    assertThat(code).isEqualTo(0)
    assertThat(out).contains("Job orders is valid")
  }

  @Test
  def unknownCommandPrintsUsage(): Unit = {
    val (code, _, err) = run("gen", "jobs/orders.conf")
    assertThat(code).isEqualTo(2)
    assertThat(err).contains("Usage")
  }
}
