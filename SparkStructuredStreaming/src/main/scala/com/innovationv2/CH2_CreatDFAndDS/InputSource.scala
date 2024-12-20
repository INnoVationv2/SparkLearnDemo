package com.innovationv2.CH2_CreatDFAndDS

import com.innovationv2.AppBase
import com.innovationv2.utils.Utils.getFilepath
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Test

class InputSource extends AppBase("InputSource") {
  @Test
  def FileSource(): Unit = {
    val lines = ss.readStream
      .format("text")
      .option("path", getFilepath("source/"))
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", getFilepath("source/"))
      .load()

    val query = lines.writeStream
      .outputMode(OutputMode.Append)
      .format("console")
      .start()

    query.awaitTermination()
  }

  @Test
  def SocketSource(): Unit = {
    val lines = ss.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val query = lines.writeStream
      .outputMode(OutputMode.Append)
      .format("console")
      .start()

    query.awaitTermination()
  }

  @Test
  def RateSource(): Unit = {
    val df = ss.readStream
      .format("rate")
      .option("rowsPerSecond", 5) // 设置每秒生成5行数据
      .load()

    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .format("console")
      .start()

    query.awaitTermination()
  }

  @Test
  def RatePerMicroBatchSource(): Unit = {
    val df = ss.readStream
      .format("rate-micro-batch")
      .option("rowsPerBatch", 5) // 设置每秒生成5行数据
      .load()

    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .format("console")
      .start()

    query.awaitTermination()
  }
}
