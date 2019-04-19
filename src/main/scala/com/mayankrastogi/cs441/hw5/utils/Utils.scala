package com.mayankrastogi.cs441.hw5.utils

import scala.io.Source

/**
  * Provides utility/helper methods for the application.
  */
object Utils {

  /**
    * Pre-compiled Regex pattern to efficiently extract authors from a publication's XML.
    */
  private val authorsPattern =
    """\<(author|editor).*>(.*)\<\/(author|editor)>""".r("startTag", "author", "endTag")

  /**
    * Pre-compiled Regex pattern to efficiently extract publication venues from a publication's XML.
    */
  private val publicationVenuePattern =
    """\<\b(journal|publisher|school|booktitle)\b>(.*)\<\/(journal|publisher|school|booktitle)>""".r("startTag", "venue", "endTag")

  /**
    * Parses the file containing the mapping of alternate and primary names of UIC CS department faculty.
    *
    * @param fileName The path relative to the resources directory for the file containing the mapping for faculty names.
    * @return Map of alternative names to primary names (the names mentioned on UIC website)
    */
  def getFacultyLookupTable(fileName: String): Map[String, String] = {
    val facultyListFile = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName))

    // Regex pattern for parsing key value pairs and capturing the keys and values using capturing groups
    val pattern =
      """(\b.+\b)\s*=\s*(\b.+\b)""".r("key", "value")

    // Match each line of input file against the regex and generate the map
    val lookupTable = facultyListFile.getLines().map(line => {
      val regexMatch = pattern.findAllIn(line)
      regexMatch.hasNext // Workaround to prevent IllegalStateException
      regexMatch.group("key") -> regexMatch.group("value")
    }).toMap

    // Close the file
    facultyListFile.close()

    lookupTable
  }

  /**
    * Finds all the authors from the given publication.
    *
    * @param publication XML string representing a single publication from the input XML file.
    * @return List of authors found in the publication. Empty if no authors could be found.
    */
  def extractAuthorsFromPublication(publication: String): Iterator[String] = {
    val regexMatch = authorsPattern.findAllMatchIn(publication)
    regexMatch.map(_.group("author"))
  }

  /**
    * Finds the publication venue of the given publication. If more than one tags are found that denote a publication
    * venue, the value of the first tag will be returned.
    *
    * @param publication XML string representing a single publication from the input XML file.
    * @return Value of the first publication venue tag found; `None` if no publication venue could be found.
    */
  def extractPublicationVenueFromPublication(publication: String): Option[String] = {
    publicationVenuePattern.findFirstMatchIn(publication).map(_.group("venue"))
  }
}
