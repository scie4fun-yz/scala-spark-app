package com.scala.spark.app.utils

import com.scala.spark.app.conf.DefaultSparkSession
import org.apache.spark.sql.DataFrame

object DataExtractor {
  val US_CITIES = "uscitiesv1.5.csv"
  val PARKING_VIOLATIONS = "parking-violations-issued-fiscal-year-2014-august-2013-june-2014.csv"
  val US_CITIES_TEST = "us-cities-test-data.csv"
  val PARKING_VIOLATIONS_TEST = "parking-violations-issued-test-data.csv"

  private val CSV_FILES_DEFAULT_LOCATION = "src\\main\\resources\\data\\"

  def extract(fileName: String): DataFrame = {
    val session = DefaultSparkSession.newSparkSession()

    val dataFrame = session.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(CSV_FILES_DEFAULT_LOCATION + fileName)

    dataFrame
  }
}
