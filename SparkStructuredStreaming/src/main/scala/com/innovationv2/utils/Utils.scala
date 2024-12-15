package com.innovationv2.utils

import com.innovationv2.Config.FILEPATH
import org.apache.spark.sql.SparkSession

object Utils {
  def getSparkSession(appName: String, confMap: Map[String, Any]): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config(confMap)
      .appName(appName)
      .getOrCreate()
  }

  def getFilepath(filename: String): String = {
    FILEPATH + filename
  }
}
