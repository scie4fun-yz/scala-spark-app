package com.scala.spark.app.conf

object DefaultHadoopLocation {

  def init(): Unit = System.setProperty("hadoop.home.dir", "C:\\hadoop")
}
