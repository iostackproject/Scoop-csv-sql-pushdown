/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.csv

import java.io.IOException

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.commons.csv._
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{Filter, BaseRelation, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types._
import com.databricks.spark.csv.util._
import com.databricks.spark.sql.readers._

case class CsvRelation protected[spark] (
  location: String,
  useHeader: Boolean,
  delimiter: Char,
  quote: Char,
  escape: Character,
  comment: Character,
  parseMode: String,
  parserLib: String,
  ignoreLeadingWhiteSpace: Boolean,
  ignoreTrailingWhiteSpace: Boolean,
  userSchema: StructType = null,
  charset: String = TextFile.DEFAULT_CHARSET.name(),
  inferCsvSchema: Boolean)(@transient val sqlContext: SQLContext)
    extends BaseRelation with PrunedFilteredScan with InsertableRelation {

  private val PUSHDOWN_ENABLEMENT_PROPERTY = "spark.pushdown.enabled"

  /**
    * Limit the number of lines we'll search for a header row that isn't comment-prefixed.
    */
  private val MAX_COMMENT_LINES_IN_HEADER = 10

  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  if((ignoreLeadingWhiteSpace || ignoreLeadingWhiteSpace) && ParserLibs.isCommonsLib(parserLib)) {
    logger.warn(s"Ignore white space options may not work with Commons parserLib option")
  }

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  val schema = inferSchema()

  def tokenRdd(header: Array[String]): RDD[Array[String]] = {

    val baseRDD = TextFile.withCharset(sqlContext.sparkContext, location, charset)

    if(ParserLibs.isUnivocityLib(parserLib)) {
      univocityParseCSV(baseRDD, header)
    } else {
      val csvFormat = CSVFormat.DEFAULT
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withEscape(escape)
        .withSkipHeaderRecord(false)
        .withHeader(header: _*)
        .withCommentMarker(comment)

      // If header is set, make sure firstLine is materialized before sending to executors.
      val filterLine = if (useHeader) firstLine else null

      baseRDD.mapPartitions { iter =>
        // When using header, any input line that equals firstLine is assumed to be header
        val csvIter = if (useHeader) {
          iter.filter(_ != filterLine)
        } else {
          iter
        }
        parseCSV(csvIter, csvFormat)
      }
    }
  }

  def tokenRdd(header: Array[String], requiredColumns:Array[String] = null, filters: String = null): RDD[Array[String]] = {

    val baseRDD = TextFile.withCharset(sqlContext.sparkContext, location, charset)

    if(ParserLibs.isUnivocityLib(parserLib)) {
      univocityParseCSV(baseRDD, header)
    } else {
      val csvFormat = CSVFormat.DEFAULT
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withEscape(escape)
        .withSkipHeaderRecord(false)
        .withHeader(header: _*)
        .withCommentMarker(comment)

      // If header is set, make sure firstLine is materialized before sending to executors.
      val filterLine = if (useHeader) firstLine else null

      logger.debug("Schema is " + schema.mkString(","))
      if (requiredColumns != null && filters != null) {
        logger.debug("Going to modify requiredColumns")
        val updatedColumns = requiredColumns.map(x => getElementID(x))
        logger.debug(updatedColumns.mkString(":"))
        val columnsMap = collection.mutable.Map[String, String]()
        requiredColumns.foreach(x => columnsMap.put(x, getElementID(x)))
        logger.debug(columnsMap.mkString(" "))
        val newFilter = censor(filters, columnsMap)


        logger.warn(newFilter)
        baseRDD.setPredicate(updatedColumns.mkString(":"), newFilter)
      }
      baseRDD.mapPartitions { iter =>
        // When using header, any input line that equals firstLine is assumed to be header
        val csvIter = if (useHeader) {
          iter.filter(_ != filterLine)
        } else {
          iter
        }
        parseCSV(csvIter, csvFormat)
      }
    }
  }

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  def buildScan = {
    logger.warn(s" CsvRelation1.2 - buildScan NO PUSHDOWN case")
    val schemaFields = schema.fields
    tokenRdd(schemaFields.map(_.name)).flatMap{ tokens =>

      if (dropMalformed && schemaFields.length != tokens.size) {
        logger.warn(s"Dropping malformed line: $tokens")
        None
      } else if (failFast && schemaFields.length != tokens.size) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: $tokens")
      } else {
        var index: Int = 0
        val rowArray = new Array[Any](schemaFields.length)
        try {
          index = 0
          while (index < schemaFields.length) {
            val field = schemaFields(index)
            rowArray(index) = TypeCast.castTo(tokens(index), field.dataType, field.nullable)
            index = index + 1
          }
          Some(Row.fromSeq(rowArray))
        } catch {
          case aiob: ArrayIndexOutOfBoundsException if permissive =>
            (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
            Some(Row.fromSeq(rowArray))
        }
      }
    }
  }

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val pushdownEnabled = getPushdownEnablement()

    if (pushdownEnabled) {
      logger.warn(s" CsvRelation1.2 - buildScan FULL PUSHDOWN case")
      val schemaFields = schema.fields
      tokenRdd(schemaFields.map(_.name), requiredColumns, filters.mkString(" ")).flatMap{ tokens =>

        if (dropMalformed && schemaFields.length != tokens.size) {
          logger.warn(s"Dropping malformed line: $tokens")
          None
        } else if (failFast && schemaFields.length != tokens.size) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: $tokens")
        } else {
          var index: Int = 0
          val rowArray = new Array[Any](schemaFields.length)
          try {
            index = 0
            while (index < schemaFields.length) {
              val field = schemaFields(index)
              rowArray(index) = TypeCast.castTo(tokens(index), field.dataType, field.nullable)
              index = index + 1
            }
            Some(Row.fromSeq(rowArray))
          } catch {
            case aiob: ArrayIndexOutOfBoundsException if permissive =>
              (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
              Some(Row.fromSeq(rowArray))
          }
        }
      }
    } else {
      buildScan14(requiredColumns)
    }
  }

  /**
    * This code was copied and slighly adapted from spark-csv 1.4 
    * it ensures that the expected data format (that is according to the requested
    * set of columns) is returned to invoker)
    * 
    * This supports to eliminate unneeded columns before producing an RDD
    * containing all of its tuples as Row objects. This reads all the tokens of each line
    * and then drop unneeded tokens without casting and type-checking by mapping
    * both the indices produced by `requiredColumns` and the ones of tokens.
    */
  private def buildScan14(requiredColumns: Array[String]): RDD[Row] = {
    val simpleDateFormatter = null
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val shouldTableScan = schemaFields.deep == requiredFields.deep
    val safeRequiredFields = if (dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    if (shouldTableScan) {
      buildScan
    } else {
      logger.warn(s" CsvRelation1.2 - buildScan local COLUMN PUSHDOWN case - spark-csv 1.4 code")
      val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => safeRequiredFields.contains(field)
      }.foreach {
        case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
      }
      val rowArray = new Array[Any](safeRequiredIndices.length)
      val requiredSize = requiredFields.length
      tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>
        if (dropMalformed && schemaFields.length != tokens.size) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        } else if (failFast && schemaFields.length != tokens.size) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        } else {
          val indexSafeTokens = if (permissive && schemaFields.length > tokens.size) {
            tokens ++ new Array[String](schemaFields.length - tokens.size)
          } else if (permissive && schemaFields.length < tokens.size) {
            tokens.take(schemaFields.size)
          } else {
            tokens
          }
          try {
            var index: Int = 0
            var subIndex: Int = 0
            while (subIndex < safeRequiredIndices.length) {
              index = safeRequiredIndices(subIndex)
              val field = schemaFields(index)
              rowArray(subIndex) = TypeCast.castTo(
                indexSafeTokens(index),
                field.dataType,
                field.nullable
              )
              subIndex = subIndex + 1
            }
            Some(Row.fromSeq(rowArray.take(requiredSize)))
          } catch {
            case _: java.lang.NumberFormatException |
                _: IllegalArgumentException if dropMalformed =>
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            case pe: java.text.ParseException if dropMalformed =>
              logger.warn("Parse exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
          }
        }
      }
    }
  }

  private def getPushdownEnablement(): Boolean = {
    // sqlEnabled is a property of the sqlContext which can be set from a jupyter notebook
    var sqlEnabled = sqlContext.getConf(PUSHDOWN_ENABLEMENT_PROPERTY, "true")
    if (sqlEnabled != None) {
      sqlEnabled = sqlEnabled.trim().toLowerCase()
    }

    logger.debug(s" CsvRelation.getPushdownEnablement() return " + sqlEnabled)
    "true".equals(sqlEnabled)
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val firstRow = if(ParserLibs.isUnivocityLib(parserLib)) {
        val escapeVal = if(escape == null) '\\' else escape.charValue()
        val commentChar: Char = if (comment == null) '\0' else comment
        new LineCsvReader(fieldSep = delimiter, quote = quote, escape = escapeVal,
          commentMarker = commentChar).parseLine(firstLine)
      } else {
        val csvFormat = CSVFormat.DEFAULT
          .withDelimiter(delimiter)
          .withQuote(quote)
          .withEscape(escape)
          .withSkipHeaderRecord(false)
        CSVParser.parse(firstLine, csvFormat).getRecords.head.toArray
      }
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index"}
      }
      if (this.inferCsvSchema) {
        InferSchema(tokenRdd(header), header)
      } else{
        // By default fields are assumed to be StringType
        val schemaFields =  header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        StructType(schemaFields)
      }
    }
  }

  /**
    * Returns the first line of the first non-empty file in path
    */
  private lazy val firstLine = {
    val csv = TextFile.withCharset(sqlContext.sparkContext, location, charset)
    if (comment == null) {
      csv.first()
    } else {
      csv.take(MAX_COMMENT_LINES_IN_HEADER)
        .find(! _.startsWith(comment.toString))
        .getOrElse(sys.error(s"No uncommented header line in " +
          s"first $MAX_COMMENT_LINES_IN_HEADER lines"))
    }
  }

  private def univocityParseCSV(
    file: RDD[String],
    header: Seq[String]): RDD[Array[String]] = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    val filterLine = if (useHeader) firstLine else null
    val dataLines = if(useHeader) file.filter(_ != filterLine) else file
    val rows = dataLines.mapPartitionsWithIndex({
      case (split, iter) => {
        val escapeVal = if(escape == null) '\\' else escape.charValue()
        val commentChar: Char = if (comment == null) '\0' else comment

        new BulkCsvReader(iter, split,
          headers = header, fieldSep = delimiter,
          quote = quote, escape = escapeVal, commentMarker = commentChar)
      }
    }, true)

    rows
  }

  private def parseCSV(
    iter: Iterator[String],
    csvFormat: CSVFormat): Iterator[Array[String]] = {
    iter.flatMap { line =>
      try {
        val records = CSVParser.parse(line, csvFormat).getRecords
        if (records.isEmpty) {
          logger.warn(s"Ignoring empty line: $line")
          None
        } else {
          Some(records.head.toArray)
        }
      } catch {
        case NonFatal(e) if !failFast =>
          logger.error(s"Exception while parsing line: $line. ", e)
          None
      }
    }
  }

  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean) = {
    val filesystemPath = new Path(location)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a CSV table:\n${e.toString}")
      }
      // Write the data. We assume that schema isn't changed, and we won't update it.
      data.saveAsCsvFile(location, Map("delimiter" -> delimiter.toString))
    } else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }

  private def getElementID(elementName : String): String = {
    val schemaFields = schema.fields
    val tmp2 = schemaFields.indexWhere(x => x.name equals(elementName))
    logger.debug(s"Found ${tmp2} for ${elementName}")
    tmp2.toString

  }

  def censor(text: String, replacements: collection.mutable.Map[String, String]): String = {
    replacements.foldLeft(text)((t, r) => t.replace(r._1, r._2))
  }
}
