package com.mayankrastogi.cs441.hw5.tests

import com.mayankrastogi.cs441.hw5.utils.{Settings, Utils}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

class UtilsTest extends FunSuite with Matchers {

  test("getFacultyLookupTable should have 52 distinct values with default configuration") {
    val settings = new Settings(ConfigFactory.load())
    val lookupTable = Utils.getFacultyLookupTable(settings.facultyListFile)
    val distinctFacultyNames = lookupTable.values.toSet

    distinctFacultyNames should have size 52
  }
}
