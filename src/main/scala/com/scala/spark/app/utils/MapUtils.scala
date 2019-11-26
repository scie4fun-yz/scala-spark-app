package com.scala.spark.app.utils

import org.apache.spark.sql.Row
import org.joda.time.format.DateTimeFormat

object MapUtils {

  def rowDateTimeToYearMonth(row: Row, dateTimeColumnName: String): Row = {
    val yearMonth = MapUtils.dateTimeToYearMonth(row.getAs[String](dateTimeColumnName))
    Row(row(0), yearMonth, row(2), row(3))
  }

  def dateTimeToYearMonth(dateTimeInput: String): String = {
    val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS"

    val format = DateTimeFormat.forPattern(pattern)

    val dateTime = format.parseDateTime(dateTimeInput)

    dateTime.getYear + "-" + dateTime.getMonthOfYear
  }

  def rowTicketsPercentage(row: Row): Row = {
    val isFirstMonth = row.getAs[String]("Year-Month").endsWith("-1")

    val currentCity = row.getAs[String]("State name")
    val previousCity = row.getAs[String]("Prev state name")
    val isNotSameState = !currentCity.equals(previousCity)

    val percentage = if (isFirstMonth || isNotSameState) "" else getPercentage(row)
    Row(row(0), row(1), row(2), row(3), row(4), percentage)
  }

  private def getPercentage(row: Row): String = {
    val current = row.getAs[Long]("Tickets").toDouble
    val previous = row.getAs[Long]("Prev month").toDouble

    roundedDouble(calculatePercentage(current, previous))
  }

  private def calculatePercentage(currentMonth: Double, previousMonth: Double): Double = {
    ((currentMonth / previousMonth) * 100) - 100
  }

  private def roundedDouble(value: Double): String = {
    var result = "0.00"
    try {
      result = BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString()
    } catch {
      case e: NumberFormatException =>
    }
    result
  }
}
