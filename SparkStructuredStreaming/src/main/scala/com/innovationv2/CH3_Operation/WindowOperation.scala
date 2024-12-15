package com.innovationv2.CH3_Operation

import com.innovationv2.AppBase
import com.innovationv2.utils.KafkaSender
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{to_timestamp, window}
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Test
import com.innovationv2.utils.KafkaSender.tsFormat

class WindowOperation extends AppBase("Window_Operations_On_EventTime") {
  private val kafkaServer: String = "localhost:9092"
  private val topic: String = s"KafkaSource-${System.currentTimeMillis() % 1000}"

  private def getInputStream: DataFrame = {
    new KafkaSender(topicPrefix = topic, msgNum = 10).start()

    val spark = ss
    import spark.implicits._
    ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("startingOffsets", "earliest")
      .option("subscribe", topic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .withColumn("word", $"key")
      .withColumn("timestamp", to_timestamp($"value", tsFormat))
  }

  @Test
  def WindowApi(): Unit = {
    val spark = ss
    import spark.implicits._
    val lines = getInputStream
      .groupBy(
        window($"timestamp", "10 seconds", "5 seconds"),
        $"word")
      .count()
      .orderBy($"window")
    outputToConsole(lines, OutputMode.Complete)
  }

  @Test
  def WindowApiWithWatermark(): Unit = {
    val spark = ss
    import spark.implicits._
    val lines = getInputStream
      .withWatermark("timestamp", "5 seconds")
      .groupBy(
        window($"timestamp", "10 seconds", "5 seconds"),
        $"word")
      .count()
      .orderBy($"window")
    outputToConsole(lines, OutputMode.Complete)
  }
}
