package org.apache.spark.sql.structured.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.structured.datasource.CustomWordTimestampSource.{dataList, gap}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

// 1. 实现DataSourceRegister接口用于注册数据源名称
class CustomWordTimestampSourceProvider extends DataSourceRegister with StreamSourceProvider {
  override def shortName(): String = "custom_word_timestamp_source"

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = {
    val schema = StructType(Array(
      StructField("word", StringType, nullable = false),
      StructField("timestamp_str", StringType, nullable = false)
    ))
    (shortName(), schema)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    new CustomWordTimestampSource(sqlContext)
  }
}

object CustomWordTimestampSource {
  private var dataList: List[Seq[(String, String)]] = List(
    Seq(("Apple", "2024-01-01 12:00:00")),
    Seq(("Dell", "2024-01-01 12:00:05")),
    Seq(("Apple", "2024-01-01 12:00:10")),
    Seq(("HP", "2024-01-01 12:00:15"))
  )
  private var gap = 1000

  def setDataList(dataList: List[Seq[(String, String)]]): Unit = {
    this.dataList = dataList
  }

  def setGapSec(gap: Int): Unit = {
    this.gap = gap * 1000
  }
}

// 2. 自定义实际的数据源类，实现核心的流数据生成逻辑
class CustomWordTimestampSource(sqlContext: SQLContext) extends SupportsTriggerAvailableNow with Source {
  private var offset: LongOffset = new LongOffset(0)

  // 返回数据源的模式（Schema）
  override def schema: StructType = StructType(Array(
    StructField("word", StringType, nullable = false),
    StructField("timestamp", StringType, nullable = false)
  ))

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException("CustomWordTimestampSource Not Support This Method")
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    Thread.sleep(gap)
    val idx = offset.json.toInt
    while (idx == dataList.length) {}

    val data: Seq[(String, String)] = dataList(idx)
    offset += 1
    val rdd = sqlContext
      .sparkContext
      .parallelize(data)
      .map { case (k, v) => InternalRow(UTF8String.fromString(k), UTF8String.fromString(v)) }
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def stop(): Unit = {}

  override def prepareForTriggerAvailableNow(): Unit = {}

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    offset
  }
}
