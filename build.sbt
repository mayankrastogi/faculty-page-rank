import scala.sys.process.Process

name := "mayank_k_rastogi_hw5"

version := "0.1"

scalaVersion := "2.11.8"

// Merge strategy to avoid deduplicate errors
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  // Typesafe Configuration Library
  "com.typesafe" % "config" % "1.3.2",

  // Apache Spark
  "org.apache.spark" %% "spark-core" % "2.4.0",

  // Scala logging framework
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",

  // Scalatest testing framework
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

// Set default main class for "sbt run" and "sbt assembly"
mainClass in(Compile, run) := Some("com.mayankrastogi.cs441.hw5.DBLPPageRank")
mainClass in assembly := Some("com.mayankrastogi.cs441.hw5.DBLPPageRank")

// Create a deployment task to automate copying the jar file to HDP sandbox and then starting the job.

lazy val deploy = taskKey[Unit]("Deploys jar file to sandbox and runs the spark job.")

deploy := {
  val log = streams.value.log

  log.info("Starting deploy task...")

  // Use Windows Subsytem for Linux (WSL) to run the shell script
  val process = Process("wsl ./deploy.sh").run(log)
  val exitCode = process.exitValue()

  if (exitCode == 0) {
    log.success("Deploy task completed successfully ")
  }
  else {
    log.error("Deploy task failed with exit code " + exitCode)
  }
}