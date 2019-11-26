package com.scala.spark.app

import com.scala.spark.app.utils.{DataExtractor, DataLoader, DataTransformer}

object ScalaSparkEtlApp extends App {

  var start = System.currentTimeMillis()
  val usCities = DataExtractor.extract(DataExtractor.US_CITIES_TEST).toDF()
  val parkingViolations = DataExtractor.extract(DataExtractor.PARKING_VIOLATIONS_TEST).toDF()
  var end = System.currentTimeMillis()
  println("Data extracted in " + (end - start) + " ms")

  start = System.currentTimeMillis()
  val transformedData = DataTransformer.transform(usCities, parkingViolations)
  end = System.currentTimeMillis()
  println("Data transformed in " + (end - start) + " ms")

  start = System.currentTimeMillis()
  DataLoader.load(transformedData)
  end = System.currentTimeMillis()
  println("Data loaded in " + (end - start) + " ms")

}
