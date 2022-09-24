package ru.philosophyit.bigdata.practice.load

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.philosophyit.bigdata.practice.Parameters

object LoadTable {
  def loadTbl(df:DataFrame, tbl:String)(implicit spark: SparkSession, parameters: Parameters): Unit ={
    import spark.implicits._
    tbl match{
      case "CALC_DME_44" =>
        val path: String = s"${parameters.PATH_PARQUET_FILES}/${tbl.replace("avedenin_vod.", "")}"
        df.withColumn("create_dm_dt", lit(parameters.CURRENT_DATE))
          .repartition('create_dm_dt)
          .write
          .option("header", "true")
//          .format("csv")
          .format("parquet")
          .partitionBy("create_dm_dt")
          .mode(SaveMode.Overwrite)
//          .csv(tbl)
          .parquet(path)

        spark.sql(s"MSCK REPAIR TABLE $tbl")

      case _ =>
        parameters.LOADING_MODE match {
          case "INITIAL" =>
            val path: String = s"${parameters.PATH_PARQUET_FILES}/${tbl.replace("avedenin_vod.", "")}"
            df.withColumn("create_dm_dt", lit(parameters.CURRENT_DATE))
              .repartition('create_dm_dt)
              .write
              .option("header", "true")
//              .format("csv")
              .format("parquet")
              .partitionBy("create_dm_dt")
              .mode(SaveMode.Overwrite)
//              .csv(tbl)
              .parquet(path)

            spark.sql(s"MSCK REPAIR TABLE $tbl")

          case "INCREMENT" =>
            val path: String = s"${parameters.PATH_PARQUET_FILES}/${tbl.replace("avedenin_vod.", "")}"
            df.withColumn("create_dm_dt", lit(parameters.CURRENT_DATE))
              .repartition('create_dm_dt)
              .write
              .option("header", "true")
//              .format("csv")
              .format("parquet")
              .partitionBy("create_dm_dt")
              .mode(SaveMode.Append)
//              .csv(tbl)
              .parquet(path)

            spark.sql(s"MSCK REPAIR TABLE $tbl")
        }
    }
  }

}
