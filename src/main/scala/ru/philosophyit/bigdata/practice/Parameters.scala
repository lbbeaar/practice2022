package ru.philosophyit.bigdata.practice

import org.apache.log4j.Logger

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime

class Parameters(args: Array[String]) extends Serializable {
  private lazy val log = Logger.getLogger(getClass)

  /**
   *
   * Метод для разделения параметров на ключ и значение
   *
   * @param arg строка с параметрами, разделёнными "="
   * @return пары ключ-значение
   */
  private def pair(arg: String): (String, String) = {
    val keyValue = arg.split("=", -1)
    try {
      (keyValue(0), keyValue(1))
    } catch {
      case indOutOf: IndexOutOfBoundsException =>
        log.info(s"NOT FOUND key for that ${keyValue(0)}\n${indOutOf.getMessage}")
        (s"NOT FOUND key", "for that " + keyValue(0))
    }
  }

  /**
   * Получение и разделение параметров на пары ключ-значение
   */
  private val argsMap: Map[String, String] = args.map(pair).toMap

  val dateCurrent: String = new SimpleDateFormat("yyyy-MM-dd")
    .format(Timestamp.valueOf(LocalDateTime.now()))

  /**
   * Получение источников собираемой витрины
   */
  val TABLE_FORM: String = argsMap.getOrElse("TABLE_FORM", "")
  val TABLE_FILIAL: String = argsMap.getOrElse("TABLE_FILIAL", "")
  val TABLE_SPECIALIZATION: String = argsMap.getOrElse("TABLE_SPECIALIZATION", "")
  val TABLE_STUDENT: String = argsMap.getOrElse("TABLE_STUDENT", "")

  /**
   * Название собираемой витрины
   */
  val TABLE_PAYMENT_DME_44: String = argsMap.getOrElse("TABLE_PAYMENT_DME_44", "")
  val TABLE_BUDGET_DME_44: String = argsMap.getOrElse("TABLE_BUDGET_DME_44", "")
  val TABLE_STUDY_DME_44: String = argsMap.getOrElse("TABLE_STUDY_DME_44", "")
  val TABLE_EXPELLED_DME_44: String = argsMap.getOrElse("TABLE_EXPELLED_DME_44", "")
  val TABLE_CALC_DME_44: String = argsMap.getOrElse("TABLE_CALC_DME_44", "")

  /**
   * Названия схемы для собираемой витрины в Hadoop
   */
  val HIVE_SCHEMA_TARGET: String = argsMap.getOrElse("HIVE_SCHEMA_TARGET", "")

  val LOADING_MODE: String = argsMap.getOrElse("LOADING_MODE", "")
  val STAGE: String = argsMap.getOrElse("STAGE", "")
  val LOG_LEVEL: String = argsMap.getOrElse("LOG_LEVEL", "ERROR")
  val PATH_PARQUET_FILES: String = argsMap.getOrElse("PATH_PARQUET_FILES", "")

  /**
   * Даты, используемые для фильтрации считываемых источников
   */
  val CURRENT_DATE: String = argsMap.getOrElse("CURRENT_DATE", dateCurrent)
}