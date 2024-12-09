package com.innovationv2.SparkSql.CH3_LoadData

import com.innovationv2.AppBase
import com.innovationv2.Config.STU_SCHEMA
import com.innovationv2.Utils.getFilepath
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.junit.Test

import java.util.Properties

class LoadData extends AppBase("LoadDataDemo") {
  @Test
  def loadDataFromRDD(): Unit = {
    val lst = List(
      (1, "Hello Spark"),
      (2, "Hello Alice"),
      (3, "Hello Bob"),
      (4, "Hello Cici"),
      (5, "Hello Doge")
    )
    val rdd: RDD[(Int, String)] = sc.makeRDD(lst)
    rdd.foreach(println)
  }

  private def loadStuCSV(): DataFrame = {
    ss.read.schema(STU_SCHEMA)
      .csv(getFilepath("stu_without_header.csv"))
  }

  @Test
  def loadCSVWithoutHeader(): Unit = {
    val df = loadStuCSV()
    df.printSchema()
    df.show()
  }

  @Test
  def loadCSVWithHeader(): Unit = {
    val df = ss.read
      .option("header", value = true)
      // 不推荐自动推断Schema，效率低，不准确
      .option("inferSchema", value = true)
      .csv(getFilepath("stu_with_header.csv"))
    df.printSchema()
    df.show()
  }

  @Test
  def loadJson(): Unit = {
    val df = ss.read.json(getFilepath("stu.json"))
    df.printSchema()
    df.show()
  }

  private def storeDataAsParquet(): Unit = {
    val df = loadStuCSV()
    df.write.parquet(getFilepath("stu_parquet"))
  }

  @Test
  def loadParquet(): Unit = {
    storeDataAsParquet()
    val df = ss.read.parquet(getFilepath("stu_parquet/"))
    df.printSchema()
    df.show()
  }

  @Test
  def loadDB(): Unit = {
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val df = ss.read
      .jdbc(url = "jdbc:mysql://localhost:3306/test",
        table = "person", props)
    df.printSchema()
    df.show()
  }
}
