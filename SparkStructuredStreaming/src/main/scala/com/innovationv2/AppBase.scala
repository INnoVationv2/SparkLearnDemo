package com.innovationv2

import com.innovationv2.utils.Utils.getSparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.OutputMode
import org.junit.{After, Before}

class AppBase(name: String) {
  var ss: SparkSession = _
  var sc: SparkContext = _
  val confMap: Map[String, Any] = Map()

  @Before
  def init(): Unit = {
    ss = getSparkSession(name, confMap)
    sc = ss.sparkContext
  }

  @After
  def destroy(): Unit = {
    sc.stop()
    ss.stop()
  }

  def outputToConsole(df: DataFrame, mode: OutputMode): Unit = {
    val query = df.writeStream
      .outputMode(mode)
      .format("console")
      .option("truncate", "false")
      .start()

    // 打印Watermark
    //    while (query.isActive) {
    //      val progress = query.lastProgress
    //      if (progress != null) {
    //        println(s"Current Watermark: ${progress.eventTime.get("watermark")}")
    //      }
    //      Thread.sleep(1000)
    //    }

    query.awaitTermination()
  }
}
