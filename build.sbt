name := "mayank_k_rastogi_hw5"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  // Typesafe Configuration Library
  "com.typesafe" % "config" % "1.3.2",

  // Logback logging framework
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.gnieh" % "logback-config" % "0.3.1",

  // Apache Spark
  "org.apache.spark" %% "spark-core" % "2.4.1",

  // Scala XML module
  "org.scala-lang.modules" %% "scala-xml" % "1.1.1",

  // Scalatest testing framework
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
)