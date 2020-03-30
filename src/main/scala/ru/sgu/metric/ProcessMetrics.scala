package ru.sgu.metric

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.SparkPubsubMessage
import ru.sgu.BikeStats

object ProcessMetrics {



  def process(stream: DStream[SparkPubsubMessage], windowInterval: Int, slidingInterval: Int,
                             spark: SparkSession) = {
    import spark.implicits._
    var i = 0
    val logger1 = Logger("name")
    stream
      .window(Seconds(windowInterval), Seconds(slidingInterval))
      .foreachRDD {
        rdd =>
          val newrdd = rdd.map(message => new String(message.getData(), StandardCharsets.UTF_8))
             .map(line => line.split(",").map(_.trim).filter(!_.isEmpty))
             .map(row => new BikeStats(BigDecimal(row(0)), row(1), row(2), BigDecimal(row(3)), BigDecimal(row(4)), BigDecimal(row(5)), row(6), BigDecimal(row(7)), row(8), row(9), row(10), row(11)))
          val DS = spark.createDataset(newrdd)

          i = i + 1
          val metrics = new MetricCalculator(spark)
          metrics.calcAggregate(DS).write.format("bigquery").option("table", "metrics.simple")
            .option("temporaryGcsBucket","ssu_practice_451_411").mode(SaveMode.Append).save()
          logger1.info("calculate four metrics")
          metrics.calcTopRentAddresses(DS, "fromStationName").write.format("bigquery").option("table", "top.from")
            .option("temporaryGcsBucket","ssu_practice_451_411").mode(SaveMode.Append).save()
          logger1.info("calculate top from addresses")
          metrics.calcTopRentAddresses(DS, "fromStationName").write.format("bigquery").option("table", "top.to")
            .option("temporaryGcsBucket","ssu_practice_451_411").mode(SaveMode.Append).save()
          logger1.info("calculate top to addresses")
          DS.write.mode(SaveMode.Append).parquet("gs://ssu_practice_451_411/output_test/")
          logger1.info("write to GS")
      }
    logger1.info(i.toString)
  }
}
