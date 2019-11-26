package com.scala.spark.app.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, count, lag, sum}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object DataTransformer {

  def transform(usCities: DataFrame, parkingViolations: DataFrame): DataFrame = {
    val schema = resultSchema()

    val aggUsCities = usCities
      .select("state_id", "state_name", "population")
      .withColumnRenamed("state_name", "State name")
      .groupBy("State name", "state_id")
      .agg(sum("population").alias("Population"))
      .sort("State name", "state_id")

    val window = org.apache.spark.sql.expressions.Window.orderBy(
      "State name", "Year-Month", "Tickets", "Population", "Ratio")

    parkingViolations.join(
      aggUsCities,
      aggUsCities("state_id") === parkingViolations("Registration State"),
      "inner"
    ).select("State name", "Issue Date", "Summons Number", "population")
      .withColumnRenamed("state_name", "State name")
      .withColumnRenamed("Issue Date", "Year-Month")
      .withColumnRenamed("population", "Population")
      .map(row => MapUtils.rowDateTimeToYearMonth(row, "Year-Month"))(RowEncoder(schema))
      .groupBy("State name", "Year-Month", "Population")
      .agg(count("Summons Number").alias("Tickets"))
      .groupBy("State name", "Year-Month", "Tickets", "Population")
      .agg((col("Tickets") / col("Population")).alias("Ratio"))
      .withColumn("Prev month", lag("Tickets", 1, 0).over(window))
      .withColumn("Prev state name", lag("State name", 1, 0).over(window))
      .map(row => MapUtils.rowTicketsPercentage(row))(RowEncoder(schema))
      .select("State name", "Year-Month", "Ratio", "Percentage")
      .sort("State name", "Year-Month")
  }

  private def resultSchema(): StructType = {
    StructType(
      StructField("State name", StringType) ::
        StructField("Year-Month", StringType) ::
        StructField("Summons Number", StringType) ::
        StructField("Population", DoubleType) ::
        StructField("Ratio", DoubleType) ::
        StructField("Percentage", StringType) ::
        Nil)
  }
}
