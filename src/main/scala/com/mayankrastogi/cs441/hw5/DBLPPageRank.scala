package com.mayankrastogi.cs441.hw5

import com.mayankrastogi.cs441.hw5.inputformats.MultiTagXmlInputFormat
import com.mayankrastogi.cs441.hw5.utils.{Settings, Utils}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Configures the Spark job and runs it.
  */
object DBLPPageRank extends LazyLogging {

  // Load application settings
  private val settings = new Settings(ConfigFactory.load())

  private val facultyLookupTable: Map[String, String] = Utils.getFacultyLookupTable(settings.facultyListFile)

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

    // If 4th argument is "local", the Spark job should be run in local mode
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
    if (localMode) {
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

    // Read publications from XML file
    val publications = spark.newAPIHadoopFile(
      inputDir,
      classOf[MultiTagXmlInputFormat],
      classOf[LongWritable],
      classOf[Text],
      hadoopConf
    ).map(_._2.toString)

    // Extract authors and publication venues from each publication
    val authorsAndVenues = processPublications(publications)
      // Cache this RDD in memory since we will be using it multiple times during page rank computation
      .cache()

    logger.info("Computing page rank...")

    // Compute page rank
    val ranks = computePageRank(authorsAndVenues, maxIterations, dampingFactor).cache()

    // Collect results
    ranks.collect()
    logger.info("Finished computing page rank ")

    // Write results
    writePageRankOutput(outputDir, ranks)

    // Stop the Spark Job
    spark.stop()
    logger.info("Finished running Spark Job")
  }

  /**
    * Extracts authors and venues from an RDD of Strings representing the XML of each publication.
    *
    * Each key in the RDD represents a node having the name of an author or a venue. Each value denotes the outgoing
    * edges from this node, i.e. all the people that this author has co-authored publications with, along with all the
    * venues where she has published.
    *
    * @param publications An RDD of Strings representing the XML of each publication.
    * @return RDD with nodes and set of their edges.
    */
  def processPublications(publications: RDD[String]): RDD[(String, Set[String])] = {
    publications
      // Extract authors and publication venues from each publication
      .flatMap(extractAuthorsAndVenueFromPublication)
      // Merge set of edges for each author removing any duplicates
      .reduceByKey(_ ++ _)
  }

  /**
    * Extracts the publication venue and a list of authors that are affiliated with UIC CS department.
    *
    * Each author links to every other author from this publication along with the publication venue, while the
    * publication venue does not have any outgoing links. If either authors or publication venue could not be extracted,
    * an empty sequence will be returned.
    *
    * @param publicationXml String representing a single publication from the input XML file
    * @return List of authors extracted from the publication paired with every other author of this publication and its
    *         venue.
    */
  def extractAuthorsAndVenueFromPublication(publicationXml: String): Seq[(String, Set[String])] = {
    logger.trace(s"extractAuthorsAndVenues(publicationElement: $publicationXml)")

    // Extract authors from the publication that are affiliated with UIC CS department
    val authors = Utils.extractAuthorsFromPublication(publicationXml).collect {
      case author if facultyLookupTable.contains(author) => facultyLookupTable(author)
    }.toSeq

    // Extract the publication venue
    val publicationVenue = Utils.extractPublicationVenueFromPublication(publicationXml)

    // If publication venue could not be extracted or the publication is not by any UIC CS department faculty,
    // return an empty sequence
    if (authors.isEmpty) {
      logger.trace("No author affiliated with UIC CS department found in the publication")
      Seq.empty
    }
    else if (publicationVenue.isEmpty) {
      logger.warn(s"No publication venue could be extracted from a publication by UIC faculty:\n$publicationXml")
      Seq.empty
    }
    // Otherwise, add every other author to the adjacency list of each author, along with the publication
    else {
      val venue = publicationVenue.get
      authors.map(author => (author, authors.filterNot(author.equals).toSet ++ Set(venue))) ++ Seq((venue, Set.empty[String]))
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
  def computePageRank(authorsAndVenues: RDD[(String, Set[String])], maxIterations: Int, dampingFactor: Double): RDD[(String, Double)] = {
    logger.trace(s"computePageRank(authorsAndVenues: $authorsAndVenues, maxIterations: $maxIterations, dampingFactor: $dampingFactor)")

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
    * @param outputDir Location where the output files and directories should be written.
    * @param ranks     An RDD containing the page rank values for all authors and publication venues.
    */
  def writePageRankOutput(outputDir: String, ranks: RDD[(String, Double)]): Unit = {
    logger.trace(s"writePageRankOutput(outputDir: $outputDir, ranks: $ranks)")

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
      .filter(node => !facultyLookupTable.contains(node._1))
      .sortBy(_._2, ascending = false, 1)
      .map({ case (node, rank) => s"$node\t$rank" })
      .saveAsTextFile(outputDirPublicationVenuesRanking)
    logger.info("Page rank for publication venues written to: " + outputDirPublicationVenuesRanking)
  }
}
