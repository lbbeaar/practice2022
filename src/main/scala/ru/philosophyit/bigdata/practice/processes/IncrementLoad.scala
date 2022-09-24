package ru.philosophyit.bigdata.practice.processes

import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import ru.philosophyit.bigdata.practice.Parameters
import ru.philosophyit.bigdata.practice.extract.ReadTables

object IncrementLoad {
  def incrementLoad()(implicit spark: SparkSession, parameters: Parameters): DataFrame ={
    import spark.implicits._

    val maxDate = if (spark.catalog.tableExists("STUDY_DME_44"))
      spark.table("STUDY_DME_44").select('max_date).head().getString(0) else "1900-01-01"

    val increment = ReadTables.form()
      .filter(date_format('date_apply, "dd.MM.yyyy") > maxDate)
    val incount = increment.count()
    spark.catalog.listTables().select('*).show()
    println(maxDate)
    println(incount)
    increment
    ReadTables.form()
  }

}
