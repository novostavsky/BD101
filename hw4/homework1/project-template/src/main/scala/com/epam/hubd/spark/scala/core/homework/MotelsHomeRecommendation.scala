package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    //set-up config from the app - setMaster("local[1]"))
    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation").setMaster("local[1]"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  /**
    * Main function that read and process the data
    * Consists of a few steps / Tasks with detailed description
    */
  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)
    rawBids.persist()

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)
    bids.persist()
    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)
    motels.persist()
    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched:RDD[EnrichedItem] = getEnriched(bids, motels)

    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  /**
    * Reads raw bids data from file for later use in transformations
    */
  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = sc.textFile(bidsPath)
    .map(line => line.split(",").toList)

  /**
    * Filter error data (w/o bids actualy)
    * and count number of such errors per (motel, date-hour) tuple
    * for later analysis went and when went wrong
    */
  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = rawBids
    .filter(line => line(2).startsWith("ERROR_"))
    .map(line => (BidError(line(1), line(2)), 1))
    .reduceByKey(_+_)
    .map(line => line._1.toString.concat(",").concat(line._2.toString))

  /**
    *Read exchange rates file to recalculate prices in EUR - output EUR rates per date
    * NOTE: the file contains EUR only, but maybe with other input data you may need additional filter
    */
  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = sc.textFile(exchangeRatesPath)
    .map(line => line.split(",").toList)
    .filter(line => line(2).equalsIgnoreCase("EUR"))
    .map(line => (line(0), line(3).toDouble))
    .collect()
    .toMap

  /**
    * Date conversion from input form used in source files to expected output format
    * NOTE: formats itself are defined in Constants object
    */
  def setDateFormat (item: BidItem): BidItem = {
    val time: DateTime = Constants.INPUT_DATE_FORMAT.parseDateTime(item.bidDate)
    return new BidItem(item.motelId, Constants.OUTPUT_DATE_FORMAT.print(time), item.loSa, item.price)
  }

  /**
    * Bid/Price conversion from US to EUR
    * NOTE: hook is used  - think if yuo need it (depends on your input data - exchange rates)
    *       if no rates for certain date - use avg hardcoded value = 0.87
    */
  def setPriceEUR (item: BidItem, exchangeRates: Map[String, Double]): BidItem = {
    val exRate: Double = exchangeRates.getOrElse(item.bidDate, 0.87) // if no ex rate by the date, use 0.87 (avg)
    return new BidItem(item.motelId, item.bidDate, item.loSa,  BigDecimal(item.price * exRate).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
  }

  /**
    * Simple check if there's no data in a cell/string has no meaningful date
    * Used in getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double])
    */
  def isEmptyTest(s:String): Boolean = (s.isEmpty || s.trim == "")

  /**
    * Get only bids we're interested in (North America  -US, CA, MX) - state data transposed/exploded
    *                                                                  from columns into rows (see flatMap step)
    * in expected format (prices in EUR + all money rounded to precision 3 +
    * + date in expected date formats - "yyyy-MM-dd HH:mm")
    */
  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = rawBids
    .filter(line => !line(2).startsWith("ERROR_"))
    .flatMap((line: List[String]) => List(
      (line(0), line(1), "US", line(5)),
      (line(0), line(1), "MX", line(6)),
      (line(0), line(1), "CA", line(8))))
    .filter((line: (String, String, String, String)) => !isEmptyTest(line._4))
    .map(item => BidItem(item._1, item._2, item._3, item._4.toDouble))
//    .filter(line => !(line.price.isNaN || line.price.isInfinity)) // no such cases
    .map(line => setPriceEUR(line, exchangeRates)) //ex rates should be calculated BEFORE dates conversion!
    .map(line => setDateFormat(line))              //date convestion should follow AFTER the exchange rates!
                                                   //the point is that ex rates use map date as a key in old format

  /**
    * Read motels data from a file
    * in order to use motel names assosiated with bids
    */
  def getMotels(sc:SparkContext, motelsPath: String): RDD[(String, String)] = sc.textFile(motelsPath)
    .map(line => line.split(",").toList)
    .map(line => (line(0), line(1)))

  /**
    * Simple check what (Losa+price) is better (where price/bid is bigger) and get that better result
    * Used in getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)])
    */
  def getMax(s1:String, v1:Double, s2:String, v2:Double): (String, Double) = {
    if(v1 > v2)
      return (s1, v1)
    else
      return (s2, v2)
  }

  /**
    * Get resulted data output -
    * Max price/bid for a hotel (used with human-readable hotel name)
    * for certain datetime stamp
    */
  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
      val mappedBids = bids.map(line => (line.motelId, line))
      val mappedMotels = motels.map(line => (line._1, line))

      return mappedBids.join(motels)
        .map(line => ((line._1, line._2._2, line.x._2.x._1.bidDate), (line.x._2.x._1.loSa, line.x._2.x._1.price)))
        .reduceByKey((v1, v2) => getMax(v1._1, v1._2, v2._1, v2._2))
        .map(line => EnrichedItem(line._1._1, line._1._2, line._1._3, line._2._1, line._2._2))
        //output format:
        //motelId: String, motelName: String, bidDate: String, loSa: String, price: Double
    }
}