package com.mayankrastogi.cs441.hw5.tests

import com.mayankrastogi.cs441.hw5.utils.{Settings, Utils}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

class UtilsTest extends FunSuite with Matchers {

  private val articleXml =
    <article mdate="2017-05-20" key="journals/ac/ChellappanD13">
      <author>Sriram Chellappan</author>
      <author>Neelanjana Dutta</author>
      <title>Mobility in Wireless Sensor Networks.</title>
      <pages>185-222</pages>
      <year>2013</year>
      <volume>90</volume>
      <journal>Advances in Computers</journal>
      <ee>https://doi.org/10.1016/B978-0-12-408091-1.00003-8</ee>
      <url>db/journals/ac/ac90.html#ChellappanD13</url>
    </article>
      .toString()

  private val proceedingsXml =
    <proceedings mdate="2017-06-08" key="tr/trier/MI99-17" publtype="informal">
      <editor>Dieter Baum</editor>
      <editor>Norbert Th. M
        &uuml;
        ller</editor>
      <editor>Richard R
        &ouml;
        dler</editor>
      <title>MMB '99, Messung, Modellierung und Bewertung von Rechen- und Kommunikationssystemen, 10. GI/NTG-Fachtagung, 22.-24. September 1999, Trier, Kurzbeitr
        &auml;
        ge und Toolbeschreibungen</title>
      <booktitle>MMB (Kurzvortr
        &auml;
        ge)</booktitle>
      <series>Universit
        &auml;
        t Trier, Mathematik/Informatik, Forschungsbericht</series>
      <volume>99-16</volume>
      <year>1999</year>
      <url>db/conf/mmb/mmb99k.html</url>
    </proceedings>
      .toString()

  private val articleWithNoAuthorOrVenue =
    <article mdate="2017-05-20" key="journals/ac/ChellappanD13">
      <title>Mobility in Wireless Sensor Networks.</title>
      <pages>185-222</pages>
      <year>2013</year>
      <volume>90</volume>
      <ee>https://doi.org/10.1016/B978-0-12-408091-1.00003-8</ee>
      <url>db/journals/ac/ac90.html#ChellappanD13</url>
    </article>
      .toString()

  private val publicationWithMultipleVenueTags =
    <proceedings mdate="2017-05-26" key="journals/thipeac/2009-2">
      <editor>Per Stenstr
        &ouml;
        m</editor>
      <title>Transactions on High-Performance Embedded Architectures and Compilers II</title>
      <volume>5470</volume>
      <year>2009</year>
      <ee>https://doi.org/10.1007/978-3-642-00904-4</ee>
      <isbn>978-3-642-00903-7</isbn>
      <booktitle>Trans. HiPEAC</booktitle>
      <series href="db/journals/lncs.html">Lecture Notes in Computer Science</series>
      <publisher>Springer</publisher>
      <url>db/journals/thipeac/thipeac2.html</url>
    </proceedings>
      .toString()

  test("getFacultyLookupTable should have 52 distinct values with default configuration") {
    val settings = new Settings(ConfigFactory.load())
    val lookupTable = Utils.getFacultyLookupTable(settings.facultyListFile)
    val distinctFacultyNames = lookupTable.values.toSet

    distinctFacultyNames should have size 52
  }

  test("getFacultyLookupTable should have a key for each value with default configuration") {
    val settings = new Settings(ConfigFactory.load())
    val lookupTable = Utils.getFacultyLookupTable(settings.facultyListFile)
    val distinctValues = lookupTable.values.toSet

    withClue(s"The following values do not have a key with same name: ${distinctValues.diff(lookupTable.keySet)}\n") {
      lookupTable.keySet should contain allElementsOf distinctValues
    }
  }

  test("extractAuthorsFromPublication should return all values of <author> tags in an <article>") {
    val authors = Utils.extractAuthorsFromPublication(articleXml).toSeq

    authors should contain allOf("Sriram Chellappan", "Neelanjana Dutta")
  }

  test("extractAuthorsFromPublication should return all values of <editor> tags in <proceedings>") {
    val editors = Utils.extractAuthorsFromPublication(proceedingsXml).toSeq

    editors should contain allOf("Dieter Baum", "Norbert Th. M&uuml;ller", "Richard R&ouml;dler")
  }

  test("extractAuthorsFromPublication should be empty if no <author> or <editor> tags are found") {
    val authors = Utils.extractAuthorsFromPublication(articleWithNoAuthorOrVenue).toSeq

    authors shouldBe empty
  }

  test("extractPublicationVenueFromPublication should return a venue when one of the venue tags are present") {
    val venueFromArticle = Utils.extractPublicationVenueFromPublication(articleXml)
    val venueFromProceedings = Utils.extractPublicationVenueFromPublication(proceedingsXml)

    venueFromArticle shouldBe defined
    venueFromArticle shouldBe Some("Advances in Computers")

    venueFromProceedings shouldBe defined
    venueFromProceedings shouldBe Some("MMB (Kurzvortr&auml;ge)")
  }

  test("extractPublicationVenueFromPublication should be None if no venue is found") {
    val venue = Utils.extractPublicationVenueFromPublication(articleWithNoAuthorOrVenue)

    venue shouldBe None
  }

  test("extractPublicationVenueFromPublication should return the venue from the first venue tag") {
    val venue = Utils.extractPublicationVenueFromPublication(publicationWithMultipleVenueTags)

    venue shouldBe defined
    venue shouldBe Some("Trans. HiPEAC")
  }
}
