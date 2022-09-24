package schemas

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Student extends Enumeration {

  val
  STUD_ID,
  SPEC_ID,
  LAST_NAME,
  NAME,
  MIDDLE_NAME,
  BIRTH_DATE,
  GENDER = Value

  val schema: StructType = StructType(
    Seq(
      StructField(STUD_ID.toString, StringType),
      StructField(SPEC_ID.toString, StringType),
      StructField(LAST_NAME.toString, StringType),
      StructField(NAME.toString, StringType),
      StructField(MIDDLE_NAME.toString, StringType),
      StructField(BIRTH_DATE.toString, StringType),
      StructField(GENDER.toString, StringType)
    )
  )
}
