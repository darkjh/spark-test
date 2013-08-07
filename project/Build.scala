import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object SparkTestBuild extends Build {
  // sbt-assembly merge-strategy settings
  def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case PathList("scala", xs @ _*) => MergeStrategy.deduplicate
      case _ => MergeStrategy.first
    }
  )

  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    // project settings
    version := "0.1.0-SNAPSHOT",
    organization := "me.juhanlol",
    scalaVersion := "2.9.3",
    // dependencies
    unmanagedBase <<= baseDirectory { base => base / "lib" },
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "1.9.1" % "test")
  )

  lazy val sparkTest = Project("spark-test", file("."),
    settings = buildSettings ++ assemblySettings)
}
