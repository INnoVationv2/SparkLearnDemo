package com.innovationv2.CH2_CreatDFAndDS

import com.innovationv2.AppBase
import com.innovationv2.utils.KafkaSender
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Test

class KafkaSource extends AppBase("kafka_source") {
  private val kafkaServer: String = "localhost:9092"
  private val topic: String = s"KafkaSource-${System.currentTimeMillis() % 1000}"

  @Test
  def createStreamingQuery(): Unit = {
    new KafkaSender(topicPrefix = topic, msgNum = 5).start()

    val df = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("startingOffsets", "earliest")
      .option("subscribe", topic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    outputToConsole(df, OutputMode.Append)
  }

  // Subscribe to multiple topics, specifying explicit Kafka offsets
  @Test
  def subscribeMultiTopic(): Unit = {
    // Spark应该接收到10条数据
    new KafkaSender(topicPrefix = topic, topicNum = 2, msgNum = 5).start()

    val df = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", s"$topic-1, $topic-2")
      //.option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    outputToConsole(df, OutputMode.Append)
  }

  // Subscribe to a pattern, at the earliest and latest offsets
  @Test
  def subscribeTopicPattern(): Unit = {
    // Spark应该接收到25条数据
    new KafkaSender(topicPrefix = topic, topicNum = 5, msgNum = 5).start()

    val df = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribePattern", s"$topic-.*")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    outputToConsole(df, OutputMode.Append)
  }
}
