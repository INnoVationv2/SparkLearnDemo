package com.innovationv2.CH3_LoadData

import com.innovationv2.AppBase
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.junit.Test
import com.innovationv2.Utils.getSourceFilepath

import java.sql.{DriverManager, ResultSet}

class LoadData extends AppBase("LoadDataDemo") {
  @Test
  def loadDataFromFS(): Unit = {
    sc.textFile(getSourceFilepath("words.txt"), 2)
      .foreach(println)
  }

  @Test
  def loadDataFromDB(): Unit = {
    val conn = () => DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    val resMapping: ResultSet => (Int, String, String, Int) = (rs: ResultSet) => {
      val id = rs.getInt(1)
      val name = rs.getString(2)
      val role = rs.getString(3)
      val salary = rs.getInt(4)
      (id, name, role, salary)
    }

    val rdd = new JdbcRDD[(Int, String, String, Int)](sc, conn,
      "select id,name,role,salary from person where ? <= id and id <= ?",
      1, 100,
      2, resMapping)
    rdd.foreach(println)
  }

  @Test
  def loadDataFromLocal(): Unit = {
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
}
