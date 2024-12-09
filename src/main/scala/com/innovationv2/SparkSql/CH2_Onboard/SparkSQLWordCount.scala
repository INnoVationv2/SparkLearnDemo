package com.innovationv2.SparkSql.CH2_Onboard

import com.innovationv2.AppBase
import org.apache.spark.sql.functions._
import org.junit.Test

class SparkSQLWordCount extends AppBase("WordCount") {
  @Test
  def wordCount(): Unit = {
    val df = ss.read.text("words.txt").toDF("line")
    df.createOrReplaceTempView("lines")
    ss.sql("SELECT explode(split(line, ' ')) AS word FROM lines")
      .groupBy("word")
      .count()
      .orderBy(desc("count"))
      .show()
  }
}

