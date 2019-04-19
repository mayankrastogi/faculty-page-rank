package com.mayankrastogi.cs441.hw5.tests

import com.mayankrastogi.cs441.hw5.DBLPPageRank
import org.scalatest.Inspectors._
import org.scalatest.{FunSuite, Matchers}

class DBLPPageRankTest extends FunSuite with Matchers {

  private val xmlWithNoUICFaculty =
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

  private val xmlWithOneUICFaculty =
    <article mdate="2017-05-20" key="journals/ac/BikasAG16">
      <author>Md. Abu Naser Bikas</author>
      <author>Abdullah Alourani</author>
      <author>Mark Grechanik</author>
      <title>How Elasticity Property Plays an Important Role in the Cloud: A Survey.</title>
      <pages>1-30</pages>
      <year>2016</year>
      <volume>103</volume>
      <journal>Advances in Computers</journal>
      <ee>https://doi.org/10.1016/bs.adcom.2016.04.001</ee>
      <url>db/journals/ac/ac103.html#BikasAG16</url>
    </article>
      .toString()

  private val xmlWithTwoUICFaculty =
    <article mdate="2017-05-27" key="journals/fmsd/SloanB97">
      <author>Ugo A. Buy</author>
      <author>Robert H. Sloan</author>
      <title>Stubborn Sets for Real-Time Petri Nets.</title>
      <pages>23-40</pages>
      <year>1997</year>
      <volume>11</volume>
      <journal>Formal Methods in System Design</journal>
      <number>1</number>
      <url>db/journals/fmsd/fmsd11.html#SloanB97</url>
      <ee>https://doi.org/10.1023/A:1008629725384</ee>
    </article>
      .toString()

  private val xmlWithFacultyWithAlternateNameListedWithPrimaryName =
    <inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
      <author>Mark Grechanik</author>
      <author>B. M. Mainul Hossain</author>
      <author>Ugo Buy</author>
      <title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
      <pages>174-183</pages>
      <year>2013</year>
      <booktitle>ICST</booktitle>
      <ee>https://doi.org/10.1109/ICST.2013.19</ee>
      <ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
      <crossref>conf/icst/2013</crossref>
      <url>db/conf/icst/icst2013.html#GrechanikHB13</url>
    </inproceedings>
      .toString()

  test("Empty sequence should be returned if no UIC faculty are part of an article") {
    val authors = DBLPPageRank.extractAuthorsAndVenues(xmlWithNoUICFaculty)

    authors shouldBe empty
  }

  test("Only UIC faculty should be extracted from a valid article") {
    val authors = DBLPPageRank.extractAuthorsAndVenues(xmlWithOneUICFaculty).map(_._1)

    authors should not be empty
    authors should contain("Mark Grechanik")
    authors should contain noneOf("Md. Abu Naser Bikas", "Abdullah Alourani")
  }

  test("UIC faculty with alternate names should get mapped with primary name when publication lists primary name") {
    val authors = DBLPPageRank.extractAuthorsAndVenues(xmlWithFacultyWithAlternateNameListedWithPrimaryName).map(_._1)

    authors should not contain "Ugo A. Buy"
    authors should contain("Ugo Buy")
  }

  test("UIC faculty with alternate names should get mapped with primary name when publication lists alternate name") {
    val authors = DBLPPageRank.extractAuthorsAndVenues(xmlWithTwoUICFaculty).map(_._1)

    authors should not contain "Ugo A. Buy"
    authors should contain("Ugo Buy")
  }

  test("UIC faculty with no alternate names should get mapped with primary name") {
    val authors = DBLPPageRank.extractAuthorsAndVenues(xmlWithOneUICFaculty).map(_._1)

    authors should contain("Mark Grechanik")
  }

  test("Adjacent nodes for an author should contain all other authors of a publication and the venue; venue should not have adjacent nodes") {
    val authorsAndVenues = DBLPPageRank.extractAuthorsAndVenues(xmlWithTwoUICFaculty)
    val venue = authorsAndVenues.find(_._1.equals("Formal Methods in System Design")).get

    venue._1 shouldBe "Formal Methods in System Design"
    venue._2 shouldBe empty

    val authors = authorsAndVenues.filterNot(venue.equals)
    authors should not contain venue

    forEvery(authors) { case (author, neighbors) =>
      neighbors should not contain author
      neighbors should contain(venue._1)
    }
  }
}
