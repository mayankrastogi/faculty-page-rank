package com.mayankrastogi.cs441.hw5.utils

import scala.io.Source

/**
  * Provides utility/helper methods for the application.
  */
object Utils {

  /**
    * Parses the file containing the mapping of alternate and primary names of UIC CS department faculty.
    *
    * @param fileName The path relative to the resources directory for the file containing the mapping for faculty names
    * @return Map of alternative names to primary names (the names mentioned on UIC website)
    */
  def getFacultyLookupTable(fileName: String): Map[String, String] = {
    val facultyListFile = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName))

    // RegEx pattern for parsing key value pairs and capturing the keys and values using capturing groups
    val pattern = """(\b.+\b)\s*=\s*(\b.+\b)""".r("key", "value")

    // Match each line of input file against the regex and generate the map
    val lookupTable = facultyListFile.getLines().map(line => {
      val regexMatch = pattern.findAllIn(line)
      regexMatch.hasNext    // Workaround to prevent IllegalStateException
      regexMatch.group("key") -> regexMatch.group("value")
    }).toMap

    // Close the file
    facultyListFile.close()

    lookupTable
  }

  /**
    * Qualifies the path to the `dblp.dtd` file.
    *
    * If an absolute path is provided, the same is returned. Otherwise, the path relative to the resources directory is
    * fully qualified into a URI and returned.
    *
    * @param absolutePath The absolute path to `dblp.dtd` file. If empty, the `resourcePath` will be used.
    * @param resourcePath The path to `dblp.dtd` relative to the resources directory.
    * @return Fully qualified URI to the file in resources, or the `absolutePath` if provided.
    */
  def getDTDFilePath(absolutePath: String, resourcePath: String = ""): String = {
    if(absolutePath != null && absolutePath.nonEmpty) {
      absolutePath
    }
    else {
      // We need to use the class loader to get the URI so that the path does not depend on whether the program is run
      // using jar file or without it
      getClass.getClassLoader.getResource(resourcePath).toURI.toString
    }
  }
}
