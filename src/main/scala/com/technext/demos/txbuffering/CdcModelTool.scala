/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.technext.demos.txbuffering

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import com.technext.demos.txbuffering.model.spec.{Ddl, InvalidJobConfigException, JobModel, ModelResolver}

/**
 * Command-line tool for job configs. Needs neither a Spark session nor Oracle.
 *
 * {{{
 *   CdcModelTool ddl jobs/<job>.conf [dir]     writes <dir>/<job>_stream.deltalake.sql and
 *                                              <dir>/<job>_current.deltalake.sql (dir: db)
 *   CdcModelTool check jobs/<job>.conf         validates the job config, printing every problem
 * }}}
 *
 * Run with `scripts/cdc-model.sh ddl jobs/orders.conf`, or from the jar with
 * `spark-submit --class com.technext.demos.txbuffering.CdcModelTool <jar> ddl jobs/orders.conf`.
 */
object CdcModelTool {

  private val Usage =
    """Usage:
      |  CdcModelTool ddl <job.conf> [dir]   write the job's stream and current table DDL (dir: db)
      |  CdcModelTool check <job.conf>       validate the job config""".stripMargin

  def main(args: Array[String]): Unit = sys.exit(run(args.toSeq, println, System.err.println))

  /** Runs a command; returns the exit code. */
  def run(args: Seq[String], out: String => Unit, err: String => Unit): Int = args match {
    case Seq("ddl", conf) => withModel(conf, err)(m => { writeDdl(m, Paths.get("db")).foreach(p => out(s"Wrote $p")); 0 })
    case Seq("ddl", conf, dir) => withModel(conf, err)(m => { writeDdl(m, Paths.get(dir)).foreach(p => out(s"Wrote $p")); 0 })
    case Seq("check", conf) => withModel(conf, err)(m => { out(s"Job ${m.name} is valid: ${m.trackedTables.mkString(", ")}"); 0 })
    case _ => err(Usage); 2
  }

  /** The two DDL files for a job's model, in `dir`. */
  def writeDdl(model: JobModel, dir: Path): Seq[Path] = {
    Files.createDirectories(dir)
    Seq(s"${model.name}_stream" -> Ddl.streamDdl(model), s"${model.name}_current" -> Ddl.currentDdl(model)).map {
      case (name, text) =>
        val p = dir.resolve(s"$name.deltalake.sql")
        Files.write(p, text.getBytes(StandardCharsets.UTF_8))
        p
    }
  }

  private def withModel(conf: String, err: String => Unit)(f: JobModel => Int): Int =
    try f(ModelResolver.load(AppConfig.loadJobFile(conf)))
    catch {
      case e: InvalidJobConfigException => err(s"$conf: ${e.getMessage}"); 1
      case e: IllegalArgumentException => err(e.getMessage); 1
    }
}
