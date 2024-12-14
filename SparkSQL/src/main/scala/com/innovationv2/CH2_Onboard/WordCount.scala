package com.innovationv2.CH2_Onboard

import com.innovationv2.AppBase
import org.apache.spark.sql.functions._
import org.junit.Test
import com.innovationv2.Utils.getSourceFilepath

class WordCount extends AppBase("WordCount") {
  @Test
  def wordCount(): Unit = {
    val df = ss.read.text(getSourceFilepath("words.txt")).toDF("line")
    df.createOrReplaceTempView("lines")
    ss.sql("SELECT explode(split(line, ' ')) AS word FROM lines")
      .groupBy("word")
      .count()
      .orderBy(desc("count"))
      .show()
  }
}

