package com.scala.spark.app.conf

import org.apache.spark.sql.SparkSession

object DefaultSparkSession {

  def newSparkSession(): SparkSession = {
    DefaultHadoopLocation.init()

    SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate
  }
}
