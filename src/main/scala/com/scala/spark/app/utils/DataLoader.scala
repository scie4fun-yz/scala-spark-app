package com.scala.spark.app.utils

import org.apache.spark.sql.DataFrame

object DataLoader {

  def load(data: DataFrame): Unit = {
    data
      .withColumnRenamed("State name", "state_name")
      .withColumnRenamed("Year-Month", "year_month")
      .withColumnRenamed("Ratio", "ratio")
      .withColumnRenamed("Percentage", "percentage")
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "public.sparketldata")
      .option("user", "postgres")
      .option("password", "root")
      .save()
  }
}
