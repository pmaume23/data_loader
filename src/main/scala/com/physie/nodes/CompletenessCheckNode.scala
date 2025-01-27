package com.physie.nodes

import com.physie.Helpers.{DataFrameReportHelper, SparkParquetHelper}
import org.slf4j.LoggerFactory

class CompletenessCheckNode{
  val logger = LoggerFactory.getLogger(getClass)

  logger.info("Initializing the completeness check node")
  //Initialize SparkSession using SparkParquetHelper
  val spark = SparkParquetHelper.getSparkSession("Stock Transactions")

  //Path to the Stock Transactions Parquet file
  val stockTransactionsPath = "src/main/resources/HDFS/credit_card_transactions/credit_card_transactions.parquet"

  //Read the Parquet file
  logger.info(s"Reading Parquet file from $stockTransactionsPath")
  val stockTransactionDF = SparkParquetHelper.readParquetFile(spark, stockTransactionsPath)

 //Generate the report
  logger.info("Generating the report")
  val reportDF = DataFrameReportHelper.generateReport(stockTransactionDF)

  //Write the report to CSV file
  val reportOutputPath = "src/main/outputReports/completenessCheckReport"
  logger.info(s"Writing the report to $reportOutputPath")
  DataFrameReportHelper.writeReportToCSV(reportOutputPath, reportDF)
}
