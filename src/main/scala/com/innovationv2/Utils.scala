package com.innovationv2

import com.innovationv2.Config.{FILEPATH, STRUCTURED_STREAMING_FILEPATH}
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

  def getStructuredStreamingPath(path: String): String = {
    STRUCTURED_STREAMING_FILEPATH + path
  }
}
