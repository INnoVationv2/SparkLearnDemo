package com.innovationv2.CH3_Operation

import com.innovationv2.AppBase
import com.innovationv2.utils.KafkaSender.tsFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{to_timestamp, window, window_time}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.structured.datasource.CustomWordTimestampSource.setDataList
import org.junit.Test

class WindowOperation extends AppBase("Window_Operations_On_EventTime") {
  private def getInputStream: DataFrame = {
    val spark = ss
    import spark.implicits._
    ss.readStream
      .format("org.apache.spark.sql.structured.datasource.CustomWordTimestampSourceProvider")
      .load()
      .withColumn("timestamp", to_timestamp($"timestamp_str", tsFormat))
  }

  @Test
  def WindowApi(): Unit = {
    val spark = ss
    import spark.implicits._
    val lines = getInputStream
      .groupBy(
        window($"timestamp", "10 seconds"),
        $"word")
      .count()
      .orderBy($"window")
    outputToConsole(lines, OutputMode.Complete)
  }

  // 区间统计是左闭右开的
  @Test
  def LeftInRightNotIn(): Unit = {
    setDataList(List(
      Seq(("Apple", "2024-01-01 12:00:00")),
      Seq(("Dell", "2024-01-01 12:00:05")),
      Seq(("Dell", "2024-01-01 12:00:07")),
      Seq(("Apple", "2024-01-01 12:00:10")),
      Seq(("Apple", "2024-01-01 12:00:13")),
      Seq(("HP", "2024-01-01 12:00:15")),
      Seq(("Apple", "2024-01-01 12:00:20")),
      Seq(("Dell", "2024-01-01 12:00:25"))
    ))
    val spark = ss
    import spark.implicits._
    val lines = getInputStream
      .groupBy(
        window($"timestamp", "10 seconds"),
        $"word")
      .count()
      .orderBy($"window")
    outputToConsole(lines, OutputMode.Complete)
  }

  // 设置水印等迟到数据5s，数据迟到了5s
  @Test
  def WatermarkIncludeDelayData(): Unit = {
    setDataList(List(
      Seq(("Apple", "2024-01-01 12:00:00")),
      Seq(("Dell", "2024-01-01 12:00:05")),
      Seq(("Apple", "2024-01-01 12:00:10")),
      Seq(("HP", "2024-01-01 12:00:15")),
      Seq(("Dell", "2024-01-01 12:00:03"))
    ))
    val spark = ss
    import spark.implicits._
    val lines = getInputStream
      .withWatermark("timestamp", "5 seconds")
      .groupBy(
        window($"timestamp", "10 seconds"),
        $"word")
      .count()
      .orderBy($"window")
    outputToConsole(lines, OutputMode.Complete)
  }

  // 设置水印等迟到数据5s，数据迟到了1天
  @Test
  def WatermarkNotIncludeDelayData(): Unit = {
    setDataList(List(
      Seq(("Apple", "2024-01-01 12:00:00")),
      Seq(("Dell", "2024-01-01 12:00:05")),
      Seq(("Apple", "2024-01-01 12:00:10")),
      Seq(("Apple", "2024-01-01 12:00:20")),

      Seq(("Apple", "2024-01-01 15:10:10")),
      Seq(("Apple", "2024-01-01 18:10:10")),
      Seq(("Apple", "2024-01-02 12:00:00")),

      Seq(("Dell", "2024-01-01 12:00:03"))
    ))
    val spark = ss
    import spark.implicits._
    val lines = getInputStream
      .withWatermark("timestamp", "5 seconds")
      .groupBy(
        window($"timestamp", "10 seconds"),
        $"word")
      .count()
    //Update和Append Mode下, Spark会清除过去的状态
    outputToConsole(lines, OutputMode.Update)

    //Complete Mode下Spark不会清除过去的状态，无论迟到多久都会更新旧数据
    //outputToConsole(lines.orderBy($"window"), OutputMode.Complete)
  }

  // windowOnWindow 需要以下设置
  override val confMap: Map[String, Any] = Map("spark.sql.streaming.statefulOperator.checkCorrectness.enabled" -> false)

  @Test
  def windowOnWindow(): Unit = {
    val spark = ss
    import spark.implicits._
    val windowedCnt = getInputStream
      .groupBy(
        window($"timestamp", "5 seconds"),
        $"word")
      .count()

    //    val anotherWindowedCnt = windowedCnt
    //      .groupBy(
    //        window(window_time($"window"), "5 mins"),
    //        $"word"
    //      ).count()

    val anotherWindowedCnt = windowedCnt
      .groupBy(
        window($"window", "20 seconds"),
        $"word"
      ).count()

    windowedCnt.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .option("truncate", "false")
      .start()

    val output = anotherWindowedCnt.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .option("truncate", "false")
      .start()

    output.awaitTermination()
  }
}
