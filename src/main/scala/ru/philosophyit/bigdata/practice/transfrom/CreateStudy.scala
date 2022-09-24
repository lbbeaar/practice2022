package ru.philosophyit.bigdata.practice.transfrom

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.philosophyit.bigdata.practice.Parameters

object CreateStudy {
  def createStudy(commonDF: DataFrame)(implicit spark: SparkSession, parameters: Parameters): DataFrame ={
    import spark.implicits._
    commonDF
      .filter('code =!= 4 && 'total_semester =!= 'max_semester)
      .withColumn("rn", row_number().over(Window.partitionBy('stud_id).orderBy('date_apply.desc)))
      .filter('rn === 1)
      .select('stud_id.cast(StringType),
        'fio.cast(StringType),
        'spec_id.cast(StringType),
        'spec_name.cast(StringType),
        'filial_name.cast(StringType),
        'form_id.cast(StringType),
        'finance.cast(StringType),
        'total_course.cast(StringType),
        'total_semester.cast(StringType),
        'status.cast(StringType),
        'code.cast(StringType),
        'max_date.cast(StringType),
        'current_date.as("current_dt").cast(StringType))
  }

}
