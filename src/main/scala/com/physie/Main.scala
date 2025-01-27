package com.physie

import com.physie.Helpers.SparkParquetHelper
import com.physie.nodes.CompletenessCheckNode
import org.slf4j.LoggerFactory

object Main extends App {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info("Starting the application")

  //Initialize SparkSession using SparkParquetHelper
  val spark = SparkParquetHelper.getSparkSession("Stock Transactions")

  //Initialize CompletenessCheckNode
  val completenessCheckNode = new CompletenessCheckNode()

  logger.info("Stopping the application")
  spark.stop()
}
