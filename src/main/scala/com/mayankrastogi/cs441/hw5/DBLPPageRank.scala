package com.mayankrastogi.cs441.hw5

import com.mayankrastogi.cs441.hw5.inputformats.MultiTagXmlInputFormat
import com.mayankrastogi.cs441.hw5.utils.{Settings, Utils}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.xml.{Elem, XML}

/**
  * Configures the Spark job and runs it.
  */
object DBLPPageRank extends LazyLogging {

  // Load application settings
  private val settings = new Settings(ConfigFactory.load())

  private val facultyLookupTable: Map[String, String] = Utils.getFacultyLookupTable(settings.facultyListFile)
  private val dblpDTDPath = Utils.getDTDFilePath(settings.dblpDTDAbsolutePath, settings.dblpDTDResourcePath)

  /**
    * Runs the spark job.
    *
    * @param args The first argument specifies the input directory. The second argument specifies the output directory.
    */
  def main(args: Array[String]): Unit = {
    logger.trace(s"main(args: ${args.mkString})")

    // Print error message if input and/or output paths are not provided as arguments
    if (args.length < 2) {
      logger.error("Input and/or output paths not provided.")
      System.exit(-1)
    }

    // Set max iterations to the 3rd argument if provided, otherwise use the one from settings
    val maxIterations = {
      try {
        if (args.length > 2) args(2).toInt else settings.maxIterations
      }
      catch {
        case _: NumberFormatException =>
          logger.warn(s"${args(2)} is not an integer. Falling back to ${settings.maxIterations} number of max iterations.")
          settings.maxIterations
      }
    }

    val localMode = args.length > 3 && args(3).equalsIgnoreCase("local")

    // Run the spark job
    runJob(args(0), args(1), maxIterations, localMode)
  }

  /**
    * Runs the Spark job.
    *
    * @param inputDir      The location of input files for the spark job.
    * @param outputDir     The location of where output of spark job will be saved.
    * @param maxIterations Maximum number of iterations for computing Page Rank.
    * @param localMode     Whether the spark job should be run in local mode.
    */
  def runJob(inputDir: String, outputDir: String, maxIterations: Int, localMode: Boolean): Unit = {
    logger.trace(s"runJob(inputPath: $inputDir, outputPath: $outputDir, maxIterations: $maxIterations, localMode: $localMode)")

    logger.info("Configuring Spark Job...")

    // Configure Spark Job and create context
    val sparkConf = new SparkConf().setAppName(settings.jobName)
    if(localMode) {
      sparkConf.setMaster("local[*]")
    }
    val spark = SparkContext.getOrCreate(sparkConf)

    // Configure XML input format
    val hadoopConf = new Configuration()
    hadoopConf.setStrings(MultiTagXmlInputFormat.START_TAG_KEY, settings.xmlInputStartTags: _*)
    hadoopConf.setStrings(MultiTagXmlInputFormat.END_TAG_KEY, settings.xmlInputEndTags: _*)

    // Get the damping factor to use for page rank calculation
    val dampingFactor = settings.dampingFactor

    logger.info("Building the Page Rank computation job")

    // Accumulator to store publication venues identified while traversing publication data
    val publicationVenuesAccumulator = spark.collectionAccumulator[String]

    // Read publications from XML file
    val publications = spark.newAPIHadoopFile(
      inputDir,
      classOf[MultiTagXmlInputFormat],
      classOf[LongWritable],
      classOf[Text],
      hadoopConf
    ).map(_._2.toString)

    // Extract authors and publication venues from each publication
    val authorsAndVenues = publications
      // Create an XML document from each publication read from the XML input file
      .map(createValidXML)
      // Extract authors and publication venues from each publication
      .flatMap(extractAuthorsAndVenues(_, publicationVenuesAccumulator))
      // Remove any duplicate entries
      .distinct()
      // Cache this RDD in memory since we will be using it multiple times during page rank computation
      .cache()

    logger.info("Computing page rank...")

    // Compute page rank
    val ranks = computePageRank(authorsAndVenues, maxIterations, dampingFactor).cache()

    // Collect results
    ranks.collect()
    val publicationVenues = publicationVenuesAccumulator.value.asScala.toSet
    logger.info("Finished computing page rank ")

    // Write results
    writePageRankOutput(outputDir, ranks, publicationVenues)

    // Stop the Spark Job
    spark.stop()
    logger.info("Finished running Spark Job")
  }

  /**
    * Wraps the input XML string between the dblp.xml's DOCTYPE so that HTML entities can be understood by the XML
    * parser.
    *
    * @param publicationXMLString The XML string received from the XmlInputFormat.
    * @return The parsed XML node with `dblp` as the root node.
    */
  def createValidXML(publicationXMLString: String): Elem = {
    logger.trace(s"createValidXML(publicationXMLString: $publicationXMLString")

    // Wrap the XML subset in a format which will be valid according to the DTD
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dblpDTDPath"><dblp>$publicationXMLString</dblp>"""

    // Parse the XML
    XML.loadString(xmlString)
  }

  /**
    * Extracts a list of authors from this publication and its publication venue. Each author links to every other
    * author from this publication along with the publication venue, while the publication venue does not have any
    * outgoing links.
    *
    * @param publicationElement The root node of the XML document (The root must be &lt;dblp&gt;)
    * @param publicationVenues  Accumulator for storing publication venues extracted from publications
    * @return List of authors extracted from the publication paired with every other author of this publication and its
    *         venue.
    */
  def extractAuthorsAndVenues(publicationElement: Elem, publicationVenues: CollectionAccumulator[String]): Seq[(String, Seq[String])] = {
    logger.trace(s"extractAuthorsAndVenues(publicationElement: $publicationElement, publicationVenues: $publicationVenues")

    // Find the type of publication
    val publicationType = publicationElement.child.head.label

    // Identify the tag which contains the value of publication venue for this type publication
    val publicationVenueLookupTag = publicationType match {
      case "article" => "journal"
      case "book" => "publisher"
      case "mastersthesis" | "phdthesis" => "school"
      case _ => "booktitle"
    }

    // Identify the tag which contains the value of authors for this type publication
    val authorLookupTag = publicationType match {
      case "book" | "proceedings" => "editor"
      case _ => "author"
    }

    try {
      // Extract the publication venue from this publication and add it to the accumulator
      val publicationVenue = (publicationElement \\ publicationVenueLookupTag).head.text

  //    publicationVenues.add(publicationVenue)

      // Extract the authors of this publication accounting for alternate names of UIC CS faculty
      //    val authors = (publicationElement \\ authorLookupTag).map(node => {
      //      if (facultyLookupTable.contains(node.text)) facultyLookupTable(node.text) else node.text
      //    })
      val authors = (publicationElement \\ authorLookupTag).collect {
        case node if facultyLookupTable.contains(node.text) => facultyLookupTable(node.text)
      }

      // The publication venue does not have any outgoing links; Each author in this publication links to every other
      // author along with the publication venue
      if(authors.nonEmpty) {
        publicationVenues.add(publicationVenue)
        Seq((publicationVenue, Seq())) ++ authors.map((_, authors ++ Seq(publicationVenue)))
      } else {
        Seq()
      }
    }
    catch {
      case _: NoSuchElementException =>
        logger.warn(s"Could not find tag <$publicationVenueLookupTag> in publication of type <$publicationType>")
        Seq()
    }
  }

  /**
    * Iteratively computes the Page Rank values from the given RDD containing author names and publication venues along
    * with their co-authorship information.
    *
    * @param authorsAndVenues RDD containing author names or publication venues as keys (nodes) and a list of author
    *                         names or publication venues as values (edges) that denote co-authorship of this node with
    *                         other nodes.
    * @param maxIterations    Maximum number of iterations for the Page Rank algorithm.
    * @param dampingFactor    Damping factor to use while computing Page Rank.
    * @return An RDD with author names or publication venues as keys and their page ranks as values.
    */
  def computePageRank(authorsAndVenues: RDD[(String, Seq[String])], maxIterations: Int, dampingFactor: Double): RDD[(String, Double)] = {
    logger.trace(s"computePageRank(authorsAndVenues: $authorsAndVenues, maxIterations: $maxIterations, dampingFactor: $dampingFactor")

    // Iterate for maxIterations times
    (1 to maxIterations)
      // Start with an initial page rank value of 1.0 for each author and publication venue
      .foldLeft(authorsAndVenues.mapValues(_ => 1.0)) { (ranks, iteration) =>
      logger.debug("Page rank computation iteration: " + iteration)

      authorsAndVenues
        // Join authors and venues with their page rank values from previous iteration
        .join(ranks)
        .values
        // Compute contribution of each node's rank towards the rank of its immediate neighbors
        .flatMap({ case (neighbors, rank) => neighbors.map((_, rank / neighbors.size)) })
        // Compute the new page rank values for each node
        .reduceByKey(_ + _).mapValues((1 - dampingFactor) + dampingFactor * _)
    }
  }

  /**
    * Writes the Page Rank values computed to disk, creating separate directories for page rank values of UIC CS faculty
    * and the publication venues. Both the lists are sorted in descending order of their page ranks.
    *
    * @param outputDir         Location where the output files and directories should be written.
    * @param ranks             An RDD containing the page rank values for all authors and publication venues.
    * @param publicationVenues Set of all publication venues identified while reading the input XML file.
    */
  def writePageRankOutput(outputDir: String, ranks: RDD[(String, Double)], publicationVenues: Set[String]): Unit = {
    logger.trace(s"writePageRankOutput(outputDir: $outputDir, ranks: $ranks, publicationVenues: $publicationVenues")

    // Clean output directory if it exists
    val outputPath = new Path(outputDir)
    logger.info("Removing any existing output files")
    val outputDirectoryDeleted = outputPath.getFileSystem(ranks.sparkContext.hadoopConfiguration).delete(outputPath, true)
    if (outputDirectoryDeleted) {
      logger.info(s"Deleted existing output files")
    }
    else {
      logger.info("Output path already clean")
    }

    // Get Page Rank values for UIC CS faculty, sort them in descending order and write the values
    val outputDirFacultyRanking = s"$outputDir/${settings.outputDirFacultyRanking}"
    ranks
      .filter(node => facultyLookupTable.contains(node._1))
      .sortBy(_._2, ascending = false, 1)
      .map({ case (node, rank) => s"$node\t$rank" })
      .saveAsTextFile(outputDirFacultyRanking)
    logger.info("Page rank for UIC CS department faculty written to: " + outputDirFacultyRanking)

    // Get Page Rank values for publication venues, sort them in descending order and write the values
    val outputDirPublicationVenuesRanking = s"$outputDir/${settings.outputDirPublicationVenuesRanking}"
    ranks
      .filter(node => publicationVenues.contains(node._1))
      .sortBy(_._2, ascending = false, 1)
      .map({ case (node, rank) => s"$node\t$rank" })
      .saveAsTextFile(outputDirPublicationVenuesRanking)
    logger.info("Page rank for publication venues written to: " + outputDirPublicationVenuesRanking)
  }
}
