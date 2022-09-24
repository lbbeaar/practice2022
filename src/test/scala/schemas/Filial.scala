package schemas

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Filial extends Enumeration {

  val
  FILIAL_ID,
  UNIVERSITY,
  FILIAL_NAME,
  FILIAL_ADDRESS,
  FILIAL_NUMBER = Value

  val schema: StructType = StructType(
    Seq(
      StructField(FILIAL_ID.toString, StringType),
      StructField(UNIVERSITY.toString, StringType),
      StructField(FILIAL_NAME.toString, StringType),
      StructField(FILIAL_ADDRESS.toString, StringType),
      StructField(FILIAL_NUMBER.toString, StringType)
    )
  )
}
