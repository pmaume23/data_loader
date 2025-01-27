package com.physie.nodes

import com.physie.Helpers.{DataFrameReportHelper, SparkParquetHelper}

class CompletenessCheckNode{
  //Initialize SparkSession using SparkParquetHelper
  val spark = SparkParquetHelper.getSparkSession("Stock Transactions")

  //Path to the Stock Transactions Parquet file
  val stockTransactionsPath = "file:///Volumes/Naophy/HDFS/stock_transactions/stock_transactions.parquet"

  //Read the Parquet file
  val stockTransactionDF = SparkParquetHelper.readParquetFile(spark, stockTransactionsPath)

  println("Stock Transactions DataFrame:")
  stockTransactionDF.show(10)
  println("Printed Stock Transactions DataFrame")

 //Generate the report
  val reportDF = DataFrameReportHelper.generateReport(stockTransactionDF)

  println("Consolidated Report:")
  reportDF.show()
  println("Printed Consolidated Report")
}
