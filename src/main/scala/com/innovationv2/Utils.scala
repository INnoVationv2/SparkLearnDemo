package com.innovationv2

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Utils {
  def getSparkContext(appName: String): SparkContext = {
    getSparkSession(appName).sparkContext
  }

  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
  }
}
