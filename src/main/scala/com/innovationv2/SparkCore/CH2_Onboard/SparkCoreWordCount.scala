package com.innovationv2.SparkCore.CH2_Onboard

import com.innovationv2.AppBase
import org.junit.Test

class SparkCoreWordCount extends AppBase("WordCount") {
  @Test
  def wordCount(): Unit = {
    sc.textFile("src/main/resources/words.txt", 2)
      .flatMap(s => s.split("\\s+"))
      .map(s => (s, 1))
      .reduceByKey((c1, c2) => c1 + c2)
      .foreach(println)
    sc.stop()
  }
}

