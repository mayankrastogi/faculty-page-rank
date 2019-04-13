package com.mayankrastogi.cs441.hw5.inputformats

import java.io.IOException
import java.nio.charset.StandardCharsets

import com.google.common.io.Closeables
import com.mayankrastogi.cs441.hw5.inputformats.MultiTagXmlInputFormat.MultiTagXmlRecordReader
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
  * Splits XML files by the specified start and end tags to send as input to Mappers.
  *
  * Produces key-value pairs of type [LongWritable, Text]. Multiple start and end tags can be specified. XML is split
  * according to whichever tag is matched first while scanning the file. To specify the start and end tags, set
  * {@link MultiTagXmlInput#START_TAG_KEY} and {@link MultiTagXmlInput#END_TAG_KEY} keys while configuring your
  * map-reduce job. Multiple tags should be comma-separated without spaces.
  *
  * This implementation is based on Apache Mahout's XMLInputFormat.
  */
class MultiTagXmlInputFormat extends TextInputFormat with LazyLogging {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    try {
      new MultiTagXmlRecordReader(split.asInstanceOf[FileSplit], context.getConfiguration)
    }
    catch {
      case ioe: IOException =>
        logger.warn("Error while creating MultiTagXmlRecordReader", ioe)
        null
    }
  }
}

/**
  * Splits XML files by the specified start and end tags to send as input to Mappers.
  *
  * Produces key-value pairs of type [LongWritable, Text]. Multiple start and end tags can be specified. XML is split
  * according to whichever tag is matched first while scanning the file. To specify the start and end tags, set
  * {@link MultiTagXmlInput#START_TAG_KEY} and {@link MultiTagXmlInput#END_TAG_KEY} keys while configuring your
  * map-reduce job. Multiple tags should be comma-separated without spaces.
  *
  * This implementation is based on Apache Mahout's XMLInputFormat.
  */
object MultiTagXmlInputFormat {

  /**
    * Key to use for specifying the start tags for splitting the XML file. Multiple tags should be comma-separated
    * without any spaces.
    */
  val START_TAG_KEY = "multitagxmlinput.start"
  /**
    * Key to use for specifying the end tags for splitting the XML file. Multiple tags should be comma-separated
    * without any spaces.
    */
  val END_TAG_KEY = "multitagxmlinput.end"

  /**
    * Reads an XML file and splits it by the specified tags.
    *
    * @param split Section of the XML file the reader will work on
    * @param conf Configuration settings specified for hadoop job
    */
  @throws[IOException]
  class MultiTagXmlRecordReader(split: FileSplit, conf: Configuration)
    extends RecordReader[LongWritable, Text] {

    // Get the start and end tags as specified by the user in the Hadoop job configuration
    private val startTags = conf.getStrings(START_TAG_KEY).map(_.getBytes(StandardCharsets.UTF_8))
    private val endTags = conf.getStrings(END_TAG_KEY).map(_.getBytes(StandardCharsets.UTF_8))

    // A map to find the corresponding end tag when the start tag is matched
    private val startTagToEndTagMapping = startTags.zip(endTags).toMap

    // Open the file and seek to the start of the split
    private val start = split.getStart
    private val end = start + split.getLength

    private val fsin = split.getPath.getFileSystem(conf).open(split.getPath)
    fsin.seek(start)

    // Buffer for storing file content between the start and end tags
    private val buffer = new DataOutputBuffer()

    // Track the current key and value
    private val currentKey = new LongWritable()
    private val currentValue = new Text()

    // Track the start tag which was matched and for which the file contents are to be buffered until the corresponding
    // end tag is encountered
    private var matchedTag = Array[Byte]()

    override def nextKeyValue(): Boolean = {
      readNext(currentKey, currentValue)
    }

    /**
      * Determines the next chunk of input file that should be sent to the mapper.
      *
      * @param key Reference to an object where the determined key will be returned
      * @param value Reference to an object where the read text will be returned
      * @throws
      * @return `true` if file was read successfully, `false` otherwise
      */
    @throws[IOException]
    private def readNext(key: LongWritable, value: Text): Boolean = {

      // Keep scanning the input file until one of the start tags is encountered
      if (fsin.getPos < end && readUntilMatch(startTags, false)) {
        try {
          // Store the matched tag in the buffer (Our output will start with the start tag)
          buffer.write(matchedTag)

          // Keep reading until the corresponding end tag is matched
          if (readUntilMatch(Array(startTagToEndTagMapping(matchedTag)), true)) {

            // Emit the key and value once the end tag is matched
            key.set(fsin.getPos)
            value.set(buffer.getData, 0, buffer.getLength)
            return true
          }
        }
        finally {
          // Reset the buffer so that we start fresh next time
          buffer.reset()
        }
      }
      false
    }

    /**
      * Scans the input stream until one of the specified tags are matched.
      *
      * The tag which got matched (from the provided list of tags) will be stored in the `matchedTag` variable once a
      * match is found.
      *
      * @param tags The tags to look for while matching. When looking for end tag, this list should contain only the
      *             end tag.
      * @param lookingForEndTag Specify whether we are looking for end tag. File contents will be buffered while the end
      *                         tag is being searched.
      * @return `true` if one of the tags is matched, `false` if end of file or end of split is reached.
      */
    private def readUntilMatch(tags: Array[Array[Byte]], lookingForEndTag: Boolean): Boolean = {
      // Trackers for the bytes that have been currently matched for each tag. Initialized to 0 at the beginning.
      val matchCounter: Array[Int] = tags.indices.map(_ => 0).toArray

      while (true) {
        // Read a byte from the input stream
        val currentByte = fsin.read()

        // Return if end of file is reached
        if (currentByte == -1) {
          return false
        }

        // If we are looking for the end tag, buffer the file contents until we find it.
        if (lookingForEndTag) {
          buffer.write(currentByte)
        }

        // Check if we are matching any of the tags
        tags.indices.foreach { tagIndex =>
          // The current tag which we are testing for a match
          val tag = tags(tagIndex)

          if (currentByte == tag(matchCounter(tagIndex))) {
            matchCounter(tagIndex) += 1

            // If the counter for this tag reaches the length of the tag, we have found a match
            if (matchCounter(tagIndex) >= tag.length) {
              matchedTag = tag
              return true
            }
          }
          else {
            // Reset the counter for this tag if the current byte doesn't match with the byte of the current tag being
            // tested
            matchCounter(tagIndex) = 0
          }
        }
        // Check if we've passed the stop point
        if (!lookingForEndTag && matchCounter.forall(_ == 0) && fsin.getPos >= end) {
          return false
        }
      }
      false
    }

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

    override def getCurrentKey: LongWritable = {
      new LongWritable(currentKey.get())
    }

    override def getCurrentValue: Text = {
      new Text(currentValue)
    }

    override def getProgress: Float = (fsin.getPos - start) / (end - start).toFloat

    override def close(): Unit = Closeables.close(fsin, true)
  }

}