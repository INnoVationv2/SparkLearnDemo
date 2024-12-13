package com.innovationv2

import org.apache.spark.sql.types.{DataTypes, StructType}

object Config {
  final val FILEPATH = "src/main/resources/"
  final val STRUCTURED_STREAMING_FILEPATH = FILEPATH + "structured_streaming/"
  final val STU_SCHEMA = new StructType()
    .add("id", DataTypes.IntegerType)
    .add("name", DataTypes.StringType)
    .add("age", DataTypes.IntegerType)
    .add("city", DataTypes.StringType)
    .add("score", DataTypes.DoubleType)
}
