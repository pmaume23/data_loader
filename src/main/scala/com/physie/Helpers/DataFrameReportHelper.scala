package com.physie.Helpers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, lit, round, when}

object DataFrameReportHelper {

  def generateReport(df: DataFrame): DataFrame = {
    val columns = df.columns

    val reportDF = columns.map { colName =>
      val notNullCount = count(when(col(colName).isNotNull, colName)).alias("Pass")
      val nullCount = count(when(col(colName).isNull, colName)).alias("Fail")
      val totalCount = count(col(colName)).alias("TotalCount")

      df.agg(notNullCount, nullCount, totalCount)
        .withColumn("ColumnName", lit(colName))
  }.reduce(_ union _)

    val reportWithPercentagesDF = reportDF
      .withColumn("PercentagePass", round((col("Pass") / col("TotalCount")) * 100, 2))
      .withColumn("PercentageFail", round((col("Fail") / col("TotalCount")) * 100, 2))

    reportWithPercentagesDF.select("ColumnName", "Pass", "Fail", "PercentagePass", "PercentageFail", "TotalCount")
  }
}
