name := "mayank_k_rastogi_hw5"

version := "0.1"

scalaVersion := "2.12.8"

// Merge strategy to avoid deduplicate errors
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  // Typesafe Configuration Library
  "com.typesafe" % "config" % "1.3.2",

  // Apache Spark with log4j dependencies removed
  "org.apache.spark" %% "spark-core" % "2.4.1" excludeAll("log4j", "slf4j-log4j12"),
  
  // Bridge log4j over slf4j
  "org.slf4j" % "log4j-over-slf4j" % "1.7.26",

  // Logback logging framework
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.gnieh" % "logback-config" % "0.3.1",
  
  // Scala XML module
  "org.scala-lang.modules" %% "scala-xml" % "1.1.1",

  // Scalatest testing framework
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
)

// Set default main class for "sbt run" and "sbt assembly"
mainClass in (Compile, run) := Some("com.mayankrastogi.cs441.hw5.pagerank.DBLPPageRankDriver")
mainClass in assembly := Some("com.mayankrastogi.cs441.hw5.pagerank.DBLPPageRankDriver")
