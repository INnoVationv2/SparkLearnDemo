package com.innovationv2

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("WordCount")
          .set("spark.kryo.registrationRequired","false")
        val sc = new SparkContext(conf)
        sc.textFile("wc.txt", 2)
          .flatMap(s => s.split("\\s+"))
          .map(s => (s, 1))
          .reduceByKey((c1, c2) => c1 + c2)
          .foreach(println)
        sc.stop()
  }
}

