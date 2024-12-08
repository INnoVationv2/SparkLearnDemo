package com.innovationv2.SparkCore.CH2_SparkOnboard

import com.innovationv2.AppBase
import org.junit.Test

class WordCount extends AppBase("WordCount") {
  @Test
  def wordCount(): Unit = {
    sc.textFile("wc.txt", 2)
      .flatMap(s => s.split("\\s+"))
      .map(s => (s, 1))
      .reduceByKey((c1, c2) => c1 + c2)
      .foreach(println)
    sc.stop()
  }
}

