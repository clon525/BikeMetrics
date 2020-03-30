import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}

import ru.sgu.metric.ProcessMetrics.process


object Main {
  def createContext() = {
    val projectID: String = "voltaic-flag-269508"
    val subcription: String = "new-cross-subscriprion"
    val windowLength: Int = 60
    val slidingInterval: Int = 20
    val appName = "BikeStats"

    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(slidingInterval))
    val logger1 = Logger("name")
    val spark: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .getOrCreate()

    val messagesStream = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "new-cross-subscriprion",
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
//      .map(message => new String(message.getData(), StandardCharsets.UTF_8))
    logger1.info("create stream")
//    val projectsStream: DStream[BikeStats] = messagesStream
//      .map(line => line.split(",").map(_.trim).filter(!_.isEmpty))
//      .map(row => BikeStats(BigDecimal(row(0)), row(1), row(2), BigDecimal(row(3)), BigDecimal(row(4)), BigDecimal(row(5)), row(6), BigDecimal(row(7)), row(8), row(9), row(10), row(11)))
//      .window(Seconds(windowLength), Seconds(slidingInterval))
//    logger1.info("rework to BikeStats DStream")

    process(messagesStream, windowLength, slidingInterval, spark)

    ssc
  }




  def main(args: Array[String]) : Unit = {
//    if (args.length != 4) {
//      System.err.println(
//        """
//          | Usage: TrendingHashtags <projectID> <windowLength> <slidingInterval> <totalRunningTime>
//          |
//          |     <projectID>: ID of Google Cloud project
//          |     <windowLength>: The duration of the window, in seconds
//          |     <slidingInterval>: The interval at which the window calculation is performed, in seconds.
//          |     <totalRunningTime>: Total running time for the application, in minutes. If 0, runs indefinitely until termination.
//          |
//        """.stripMargin)
//      System.exit(1)
//    }

    //val Seq(projectID, windowInterval, slidingInterval, totalRunningTime) = args.toSeq
    val logger1 = Logger("name")
    logger1.info("create logger")
    val ssc = createContext()
    logger1.info("create context")

    ssc.start()             // Start the computation
    logger1.info("finish")

  }
}
