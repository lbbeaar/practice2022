package ru.philosophyit.bigdata.practice.extract

import ru.philosophyit.bigdata.practice.Parameters
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{concat, lit, to_date}

object ReadTables{

  def form()(implicit spark: SparkSession, parameters: Parameters): DataFrame = {
    import spark.implicits._
    spark.table(parameters.TABLE_FORM)
      .select('stud_id, 'form_id, 'form_of_education, 'finance, 'course, to_date('education_start, "dd.MM.yyyy").as("education_start"), to_date('date_apply, "dd.MM.yyyy").as("date_apply"), 'code)
  }

  def filial()(implicit spark: SparkSession, parameters: Parameters): DataFrame = {
    import spark.implicits._
    spark.table(parameters.TABLE_FILIAL)
      .select('filial_id, 'filial_name)
  }

  def specialization()(implicit spark: SparkSession, parameters: Parameters): DataFrame = {
    import spark.implicits._
    spark.table(parameters.TABLE_SPECIALIZATION)
      .select('spec_id, 'filial_id, 'spec_name, 'years, 'cost)
  }

  def student()(implicit spark: SparkSession, parameters: Parameters): DataFrame = {
    import spark.implicits._
    spark.table(parameters.TABLE_STUDENT)
      .withColumn("fio", concat('last_name, lit(" "), 'name, lit(" "), 'middle_name))
      .select('stud_id, 'spec_id, 'fio)
  }
}
