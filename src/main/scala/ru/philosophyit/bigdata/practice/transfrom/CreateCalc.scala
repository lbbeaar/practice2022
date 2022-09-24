package ru.philosophyit.bigdata.practice.transfrom

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, lit, row_number}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.philosophyit.bigdata.practice.Parameters

object CreateCalc {
  def createCalc(commonDF: DataFrame)(implicit spark: SparkSession, parameters: Parameters): DataFrame ={
    import spark.implicits._

    val temp = commonDF.filter('code =!= 4)
      .withColumn("rn", row_number().over(Window.partitionBy('stud_id).orderBy('stud_id)))
      .filter('rn === 1)
      .select('spec_id, 'fio, 'spec_name, 'filial_id, 'filial_name, 'total_course, 'rn)


    val spec: DataFrame = temp.groupBy('spec_id, 'spec_name)
      .agg(count('spec_id).as("count"))
      .select(lit("SPEC").as("stg"), 'spec_id.as("id"), 'spec_name.as("name"), 'count.as("count_calc"))

    val filial: DataFrame = temp.groupBy('filial_id, 'filial_name)
      .agg(count('filial_id).as("count"))
      .select(lit("FILIAL").as("stg"), 'filial_id.as("id"), 'filial_name.as("name"), 'count.as("count_calc"))

    val course: DataFrame = temp.groupBy('total_course)
      .agg(count('total_course).as("count"))
      .select(lit("COURSE").as("stg"), 'total_course.as("id"), lit("курс").as("name"), 'count.as("count_calc"))

    val stg4: DataFrame = spec.union(filial)
      .union(course)

    stg4.select('stg.cast(StringType), 'id.cast(StringType), 'name.cast(StringType), 'count_calc.cast(StringType))
  }
}
