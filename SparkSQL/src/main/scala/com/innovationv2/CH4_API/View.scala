package com.innovationv2.CH4_API

import com.innovationv2.AppBase
import com.innovationv2.Config.STU_SCHEMA
import com.innovationv2.Utils.getSourceFilepath
import org.apache.spark.sql.AnalysisException
import org.junit.Test

class View extends AppBase("ViewDemo") {
  @Test
  def View(): Unit = {
    val df = ss.read.schema(STU_SCHEMA)
      .csv(getSourceFilepath("stu_without_header.csv"))
    df.createGlobalTempView("GlobalStu")
    df.createTempView("SessionStu")

    ss.sql("select * from global_temp.GlobalStu").show()
    ss.sql("select * from SessionStu").show()

    val ss2 = ss.newSession()
    ss2.sql("select * from global_temp.GlobalStu").show()
    try {
      ss2.sql("select * from SessionStu").show()
    } catch {
      case _: AnalysisException =>
        println("Table `SessionStu` Not Found")
    }
  }
}
