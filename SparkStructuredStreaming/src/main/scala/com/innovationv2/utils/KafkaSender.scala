package com.innovationv2.utils

import com.innovationv2.AppBase
import com.innovationv2.utils.KafkaSender.tsFormatter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object KafkaSender {
  final val tsFormat = "yyyy-MM-dd HH:mm:ss"
  final val tsFormatter = DateTimeFormatter.ofPattern(tsFormat)
}

class KafkaSender(topicPrefix: String, topicNum: Int = 1, msgNum: Int = 100, gapSec: Int = 1, var conf: Properties = null) extends AppBase("kafka_source") {
  private final val words = Array("hello", "world", "spark", "flink", "Apple")
  private var producer: KafkaProducer[String, String] = null

  private def _init(): Unit = {
    if (conf == null) {
      conf = new Properties()
      conf.put("bootstrap.servers", "localhost:9092") // 替换为你的Kafka服务器地址
      conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    }
    producer = new KafkaProducer[String, String](conf)
  }

  private def getWord: String = {
    words(new Random().nextInt(words.length))
  }

  def start(): Unit = {
    _init()
    if (topicNum == 1) {
      Future {
        writeDataToKafka(s"$topicPrefix")
      }
    } else {
      for (i <- 1 to topicNum) {
        Future {
          writeDataToKafka(s"$topicPrefix-$i")
        }
      }
    }
  }

  private def writeDataToKafka(topic: String): Unit = {
    println(s"Write Data To $topic")

    var curTS = Instant.now().atZone(ZoneId.systemDefault())
    try {
      for (_ <- 1 to msgNum) {
        val key = getWord
        curTS = curTS.plusSeconds(gapSec)
        val value = curTS.format(tsFormatter)
        val record = new ProducerRecord[String, String](topic, key, value)
        producer.send(record)
        println(s"Sent:$key->$value")
        Thread.sleep(gapSec * 1000)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      producer.close()
    }
  }
}
