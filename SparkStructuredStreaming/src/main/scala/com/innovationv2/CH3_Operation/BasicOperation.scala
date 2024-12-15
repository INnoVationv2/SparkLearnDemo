package com.innovationv2.CH3_Operation

import com.innovationv2.AppBase
import com.innovationv2.utils.Utils.getFilepath
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructType}
import org.junit.Test

import java.sql.Date

case class DeviceData(device: String, deviceType: String, signal: Double, time: Date)

class BasicOperation extends AppBase("BasicOperation") {
  private final val deviceSchema = new StructType()
    .add("device", StringType)
    .add("deviceType", StringType)
    .add("signal", DoubleType)
    .add("time", DateType)

  private def input(): DataFrame = {
    ss.readStream
      .format("csv")
      .option("path", getFilepath("/csv/device_data/"))
      .option("header", value = true)
      .schema(deviceSchema)
      .load()
  }

  @Test
  def unTypedAPI(): Unit = {
    var df = input()
    df = df.where("signal > 10")
      .groupBy("deviceType")
      .avg("signal")

    df.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .start()
      .awaitTermination()
  }

  @Test
  def typedAPI(): Unit = {
    val spark = ss
    import spark.implicits._
    val ds = input().as[DeviceData]
    import org.apache.spark.sql.expressions.scalalang.typed
    val ds_2 = ds.filter(_.signal > 10)
      .groupByKey(_.deviceType)
      .agg(typed.avg(_.signal))

    ds.groupByKey(_.deviceType)
      .agg(typed.avg(_.signal))
      .show()
  }
}
