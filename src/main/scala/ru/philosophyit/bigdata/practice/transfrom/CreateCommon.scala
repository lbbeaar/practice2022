package ru.philosophyit.bigdata.practice.transfrom

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{ceil, concat, current_date, date_format, lit, max, month, when, year}
import org.apache.spark.sql.types.IntegerType
import ru.philosophyit.bigdata.practice.Parameters
import ru.philosophyit.bigdata.practice.extract.ReadTables
import ru.philosophyit.bigdata.practice.processes.IncrementLoad

object CreateCommon {
  def createCommon()(implicit spark: SparkSession, parameters: Parameters): DataFrame ={
    import spark.implicits._

    val filial = ReadTables.filial()
    val form = IncrementLoad.incrementLoad()
      .withColumn("current_date", current_date)
      .withColumn("current_date_semester", when(month('current_date) <= 6, 2).otherwise(1))
      .withColumn("date_apply_semester", when(month('date_apply) <= 6, 0).otherwise(1))
      .withColumn("total_semester",
        when('code === 4, (year('date_apply) - year('education_start)) * 2 + 'date_apply_semester)
          .otherwise((year('current_date) - year('education_start)) * 2 + 'current_date_semester))
      .withColumn("total_pay_semester",
        when('finance === "Д",
          when(year('date_apply) === year('education_start), 1)
            .otherwise('total_semester - ((year('current_date) - year('date_apply)) * 2) + 'date_apply_semester))
          .otherwise(null))
    val spec = ReadTables.specialization()
    val stud = ReadTables.student()

    val allFilial = spec
      .join(filial, Seq("filial_id"))

    val result = stud
      .join(form, Seq("stud_id"), "left")
      .join(allFilial, Seq("spec_id"))
      .withColumn("total_semester", when('total_semester > 'years * 2, 'years * 2)
        .otherwise('total_semester).cast(IntegerType))
      .withColumn("total_course", ceil('total_semester / 2))
      .withColumn("max_semester", when('form_of_education === "Очная", 8).otherwise(10))
      .withColumn("status",
        when('code === 4,
          concat(lit("Отчислен на "), 'total_course, lit(" курсе "), 'total_semester, lit(" семестре")))
          .when('total_semester >= 'max_semester,
            concat(lit(s"Закончил обучение на "), 'years, lit(" курсе "), 'max_semester, lit(" семестре")))
          .otherwise(concat(lit("Обучается на "), 'total_course, lit(" курсе "), 'total_semester, lit(" семестре"))))
      .withColumn("max_date", date_format(max('date_apply).over(), "dd.MM.yyyy"))
    result

  }
}
