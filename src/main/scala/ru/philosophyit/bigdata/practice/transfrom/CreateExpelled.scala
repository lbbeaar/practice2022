package ru.philosophyit.bigdata.practice.transfrom

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.philosophyit.bigdata.practice.Parameters

object CreateExpelled {
  def createExpelled(commonDF: DataFrame)(implicit spark: SparkSession, parameters: Parameters): DataFrame ={
    import spark.implicits._
    commonDF
      .filter('code === "4" || 'total_semester === 'max_semester)
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
        'current_date.as("current_dt").cast(StringType))
  }
}
