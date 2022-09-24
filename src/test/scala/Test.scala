import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.philosophyit.bigdata.practice.Main.main_spark
import ru.philosophyit.bigdata.practice.Parameters
import ru.philosophyit.bigdata.practice.extract.ReadTables
import ru.philosophyit.bigdata.practice.transfrom._
import utils.TestUtils

class Test extends TestUtils{

  test("practice") {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("test_practice")
      .getOrCreate()

    implicit val parameters: Parameters = new Parameters(argsLocal)

    spark.sparkContext.setLogLevel(parameters.LOG_LEVEL)
    initTables(parameters)

    main_spark(argsLocal)

//    val commonDF: DataFrame = CreateCommon.createCommon()
//    val paymentDF: DataFrame = CreatePayment.createPayment(commonDF)
//    val budgetDF: DataFrame = CreateBudget.createBudget(commonDF)
//    val expelledDF: DataFrame = CreateExpelled.createExpelled(commonDF)
//    val studyDF: DataFrame = CreateStudy.createStudy(commonDF)
//    val calc: DataFrame = CreateCalc.createCalc(commonDF)
//    commonDF.show(numRows = 40, false)
//    paymentDF.show(false)
//    budgetDF.show(100, false)
//    expelledDF.show(false)
//    studyDF.show(30, false)
//    calc.show(1000, false)



    // Нужно удалить папку spark-warehouse если она есть
//    val payment = spark.read.parquet("PAYMENT_DME_44")
//    val budget = spark.read.parquet("BUDGET_DME_44")
//    val study = spark.read.parquet("STUDY_DME_44")
//    val expelled = spark.read.parquet("EXPELLED_DME_44")
//    val calc = spark.read.parquet("CALC_DME_44")

    // Запись итоговых таблиц в файлы csv
//    payment.repartition(1).write
//      .option("header", "true")
//      .option("delimiter", ";")
//      .mode("append")
//      .format("csv")
//      .save("src/test/resources/output/payment")
//
//    budget.repartition(1).write
//      .option("header", "true")
//      .option("delimiter", ";")
//      .mode("append")
//      .format("csv")
//      .save("src/test/resources/output/budget")

//    study.repartition(1).write
//      .option("header", "true")
//      .option("delimiter", ";")
//      .mode("append")
//      .format("csv")
//      .save("src/test/resources/output/study")

//    expelled.repartition(1).write
//      .option("header", "true")
//      .option("delimiter", ";")
//      .mode("append")
//      .format("csv")
//      .save("src/test/resources/output/expelled")
//
//    calc.repartition(1).write
//      .option("header", "true")
//      .option("delimiter", ";")
//      .mode("append")
//      .format("csv")
//      .save("src/test/resources/output/calc")
  }
}
