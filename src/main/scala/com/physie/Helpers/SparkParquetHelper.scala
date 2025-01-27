package com.physie.Helpers

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkParquetHelper {
  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  def readParquetFile(spark: SparkSession, filePath: String): DataFrame = {
    spark.read.parquet(filePath)
  }
}
