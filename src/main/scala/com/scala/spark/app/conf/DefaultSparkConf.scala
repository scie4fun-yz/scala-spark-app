package com.scala.spark.app.conf

import org.apache.spark.SparkConf

object DefaultSparkConf {

  def newSparkConfig(): SparkConf = {
    DefaultHadoopLocation.init()

    new SparkConf()
      .setAppName("Scala Spark App")
      .set("spark.driver.allowMultipleContexts", "true")
      .setMaster("local")
  }
}
