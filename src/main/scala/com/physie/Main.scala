package com.physie

import com.physie.Helpers.SparkParquetHelper
import com.physie.nodes.CompletenessCheckNode

object Main extends App {

  //Initialize SparkSession using SparkParquetHelper
  val spark = SparkParquetHelper.getSparkSession("Stock Transactions")

  //Initialize CompletenessCheckNode
  val completenessCheckNode = new CompletenessCheckNode()
  completenessCheckNode

  spark.stop()
}
