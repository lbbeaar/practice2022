package schemas

import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object Form extends Enumeration {

  val
  STUD_ID,
  FORM_ID,
  FORM_OF_EDUCATION,
  FINANCE,
  COURSE,
  EDUCATION_START,
  DATE_APPLY,
  CODE = Value

  val schema: StructType = StructType(
    Seq(
      StructField(STUD_ID.toString, StringType),
      StructField(FORM_ID.toString, StringType),
      StructField(FORM_OF_EDUCATION.toString, StringType),
      StructField(FINANCE.toString, StringType),
      StructField(COURSE.toString, StringType),
      StructField(EDUCATION_START.toString, StringType),
      StructField(DATE_APPLY.toString, StringType),
      StructField(CODE.toString, StringType)
    )
  )
}
