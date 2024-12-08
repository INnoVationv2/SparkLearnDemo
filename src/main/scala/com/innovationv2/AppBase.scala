package com.innovationv2

import com.innovationv2.Utils.getSparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.junit.{After, Before}

class AppBase(name: String) {
  var ss: SparkSession = _
  var sc: SparkContext = _

  @Before
  def init(): Unit = {
    ss = getSparkSession(name)
    sc = ss.sparkContext
  }

  @After
  def destroy(): Unit = {
    sc.stop()
    ss.stop()
  }
}
