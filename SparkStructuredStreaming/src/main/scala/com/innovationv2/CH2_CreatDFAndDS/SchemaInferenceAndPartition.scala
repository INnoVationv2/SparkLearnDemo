package com.innovationv2.CH2_CreatDFAndDS

import com.innovationv2.AppBase
import com.innovationv2.Utils.getFilepath
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.junit.Test

class SchemaInferenceAndPartition extends AppBase("SchemaInferenceAndPartition") {
  override val confMap: Map[String, Any] = Map("spark.sql.streaming.schemaInference" -> true)

  @Test
  def inferSchema(): Unit = {
    val df = ss.readStream
      .format("csv")
      .option("path", getFilepath("csv/with_header/"))
      .option("header", value = true)
      .load()

    df.printSchema()

    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .format("console")
      .start()

    query.awaitTermination()
  }

  @Test
  def partition(): Unit = {
    val userSchema = new StructType()
      .add("year", "integer")
      .add("month", "integer")
      .add("product_name", "string")
      .add("quantity_sold", "integer")
      .add("price", "double")

    val df = ss.readStream
      .format("csv")
      .option("path", getFilepath("/csv/sales_data/"))
      .option("header", value = true)
      .option("sep", ",")
      .schema(userSchema)
      .load()

    df.printSchema()

    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .format("console")
      .start()

    query.awaitTermination()
  }
}
