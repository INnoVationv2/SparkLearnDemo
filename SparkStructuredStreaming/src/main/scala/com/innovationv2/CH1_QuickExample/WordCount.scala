package com.innovationv2.CH1_QuickExample

import com.innovationv2.AppBase
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Test

class WordCount extends AppBase("Demo") {
  @Test
  def wordCount(): Unit = {
    val spark = ss
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._
    val wordCounts = lines.as[String]
      .flatMap(_.split(" "))
      .groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .start()

    query.awaitTermination()
  }
}
