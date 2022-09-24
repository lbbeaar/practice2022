package utils

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.scalatest.FunSuite
import ru.philosophyit.bigdata.practice.Parameters
import schemas._

trait TestUtils extends FunSuite with DatasetSuiteBase{

  val argsLocal: Array[String] = Array(
    "TABLE_FORM=FORM",
    "TABLE_FILIAL=FILIAL",
    "TABLE_SPECIALIZATION=SPECIALIZATION",
    "TABLE_STUDENT=STUDENT",

    "TABLE_PAYMENT_DME_44=PAYMENT_DME_44",
    "TABLE_BUDGET_DME_44=BUDGET_DME_44",
    "TABLE_STUDY_DME_44=STUDY_DME_44",
    "TABLE_EXPELLED_DME_44=EXPELLED_DME_44",
    "TABLE_CALC_DME_44=CALC_DME_44",

    "HIVE_SCHEMA_TARGET=",

    "STAGE=local",
    "LOADING_MODE=INITIAL",
    "PATH_PARQUET_FILES=src/test/resources/result"

  )

  def createTableFromFile(tableName: String, schema: StructType, delimiter: String = ";")
                         (implicit spark: SparkSession): Unit = {
    val path = s"src/test/resources/input/$tableName.csv"
    spark.read.format("csv")
      .option("header", value = true)
      .option("nullValue", null)
      .option("delimiter", delimiter)
      .schema(schema)
      .load(path)
      .createOrReplaceTempView(tableName)
  }

  def initTables(parameters: Parameters)(implicit spark: SparkSession): Unit = {
    createTableFromFile(parameters.TABLE_FILIAL, Filial.schema)
    createTableFromFile(parameters.TABLE_FORM, Form.schema)
    createTableFromFile(parameters.TABLE_SPECIALIZATION, Specialization.schema)
    createTableFromFile(parameters.TABLE_STUDENT, Student.schema)
  }

}

