package com.mayankrastogi.cs441.hw5.utils

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._

/**
  * Maps and holds configuration read from config files for easy access.
  *
  * @param config The { @link Config} object that holds parameters read from a config file
  */
class Settings(config: Config) {

  // Validate provided config file against the reference (default) config file.
  // Exceptions are thrown if validation fails
  config.checkValid(ConfigFactory.defaultReference(), Settings.CONFIG_NAMESPACE)

  // Application settings

  val facultyListFile: String = getString("faculty-list-file")

  // DBLP DTD File Location

  val dblpDTDAbsolutePath: String = getString("dblp-dtd.absolute-path")
  val dblpDTDResourcePath: String = getString("dblp-dtd.resource-path")

  // Spark Job Config

  val jobName: String = getString("job.name")
  val xmlInputStartTags: List[String] = getStringList("xml-input.start-tags")
  val xmlInputEndTags: List[String] = getStringList("xml-input.end-tags")

  // ===================================================================================================================
  // Private Helpers
  // ===================================================================================================================

  private def getBoolean(path: String): Boolean = config.getBoolean(configPath(path))

  private def getDouble(path: String): Double = config.getDouble(configPath(path))

  private def getInt(path: String): Int = config.getInt(configPath(path))

  private def getIntList(path: String): List[Int] = config.getIntList(configPath(path)).asScala.toList.asInstanceOf[List[Int]]

  private def getLong(path: String): Long = config.getLong(configPath(path))

  private def getString(path: String): String = config.getString(configPath(path))

  private def getStringList(path: String): List[String] = config.getStringList(configPath(path)).asScala.toList

  /** Prefixes root key to the specified path to avoid typing it each time a parameter is fetched. */
  private def configPath(path: String): String = s"${Settings.CONFIG_NAMESPACE}.$path"
}

/** Companion object to define "static" members for Settings class */
object Settings {

  /** Root key for configuration */
  val CONFIG_NAMESPACE: String = "dblp-page-rank"

}