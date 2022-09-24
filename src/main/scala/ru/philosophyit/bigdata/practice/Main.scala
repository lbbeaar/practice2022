package ru.philosophyit.bigdata.practice

import org.apache.spark.sql.SparkSession
import ru.philosophyit.bigdata.practice.load.LoadTable
import ru.philosophyit.bigdata.practice.transfrom.{CreateBudget, CreateCalc, CreateCommon, CreateExpelled, CreatePayment, CreateStudy}

object Main extends App {
//  def main_spark(args: Array[String]): Unit = {

    implicit val parameters: Parameters = new Parameters(args)

    implicit val spark: SparkSession = SparkSession
      .builder()
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel(parameters.LOG_LEVEL)
    val common = CreateCommon.createCommon()
//    common.show(100, false)

//    parameters.STAGE match {
//      case "local" =>
//        val budget = CreateBudget.createBudget(common)
//        LoadTable.loadTbl(budget, parameters.TABLE_BUDGET_DME_44)
//        val calc = CreateCalc.createCalc(common)
//        LoadTable.loadTbl(calc, parameters.TABLE_CALC_DME_44)
//        val expelled = CreateExpelled.createExpelled(common)
//        LoadTable.loadTbl(expelled, parameters.TABLE_EXPELLED_DME_44)
//        val payment = CreatePayment.createPayment(common)
//        LoadTable.loadTbl(payment, parameters.TABLE_PAYMENT_DME_44)
//        val study = CreateStudy.createStudy(common)
//        LoadTable.loadTbl(study, parameters.TABLE_STUDY_DME_44)
//    }

    parameters.STAGE match {
      case "datamart_budget" =>
        val budget = CreateBudget.createBudget(common)
        LoadTable.loadTbl(budget, parameters.TABLE_BUDGET_DME_44)
      case "datamart_calc" =>
        val calc = CreateCalc.createCalc(common)
        LoadTable.loadTbl(calc, parameters.TABLE_CALC_DME_44)
      case "datamart_expelled" =>
        val expelled = CreateExpelled.createExpelled(common)
        LoadTable.loadTbl(expelled, parameters.TABLE_EXPELLED_DME_44)
      case "datamart_payment" =>
        val payment = CreatePayment.createPayment(common)
        LoadTable.loadTbl(payment, parameters.TABLE_PAYMENT_DME_44)
      case "datamart_study" =>
        val study = CreateStudy.createStudy(common)
        LoadTable.loadTbl(study, parameters.TABLE_STUDY_DME_44)
    }
//  }
}