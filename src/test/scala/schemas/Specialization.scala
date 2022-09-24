package schemas

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Specialization extends Enumeration {

  val
  SPEC_ID,
  FILIAL_ID,
  SPEC_NAME,
  YEARS,
  COST,
  QUOTA = Value

  val schema: StructType = StructType(
    Seq(
      StructField(SPEC_ID.toString, StringType),
      StructField(FILIAL_ID.toString, StringType),
      StructField(SPEC_NAME.toString, StringType),
      StructField(YEARS.toString, StringType),
      StructField(COST.toString, StringType),
      StructField(QUOTA.toString, StringType)
    )
  )
}
