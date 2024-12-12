package com.innovationv2.SparkStructuredStreaming.CH1_QuickExample

import com.innovationv2.AppBase
import org.junit.Test

class StreamWordCount extends AppBase("Demo") {
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
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
