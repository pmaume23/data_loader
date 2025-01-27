package com.physie.Helpers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, lit, round, when}

object DataFrameReportHelper {

  def generateReport(df: DataFrame): DataFrame = {
    val columns = df.columns

    val reportDF = columns.map { colName =>
      val notNullCount = count(when(col(colName).isNotNull, colName)).alias("Pass")
      val nullCount = count(when(col(colName).isNull, colName)).alias("Fail")
      val totalCount = (notNullCount + nullCount).alias("TotalCount")

      df.agg(notNullCount, nullCount, totalCount)
        .withColumn("ColumnName", lit(colName))
  }.reduce(_ union _)

    val reportWithPercentagesDF = reportDF
      .withColumn("PercentagePass", round((col("Pass") / col("TotalCount")) * 100, 2))
      .withColumn("PercentageFail", round((col("Fail") / col("TotalCount")) * 100, 2))

    reportWithPercentagesDF.select("ColumnName", "Pass", "Fail", "PercentagePass", "PercentageFail", "TotalCount")
  }

  // Method to write the report to CSV
  def writeReportToCSV(outputPath: String, df: DataFrame): Unit = {
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(outputPath)
    val tempDir = new java.io.File(outputPath)
    val tempFile = tempDir.listFiles().filter(_.getName.endsWith(".csv")).head
    tempFile.renameTo(new java.io.File(outputPath + ".csv"))
    tempDir.listFiles().foreach(_.delete())
    tempDir.delete()
  }
}
