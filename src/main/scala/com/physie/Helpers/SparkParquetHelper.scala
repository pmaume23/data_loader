package com.physie.Helpers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object SparkParquetHelper {
  val logger = LoggerFactory.getLogger(getClass)
  def getSparkSession(appName: String): SparkSession = {
    logger.info(s"Creating SparkSession with app name: $appName")
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  def readParquetFile(spark: SparkSession, filePath: String): DataFrame = {
    logger.info(s"Reading Parquet file from $filePath")
    spark.read.parquet(filePath)
  }
}
