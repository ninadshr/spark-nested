package org.spark.nested

import org.apache.spark.sql.SparkSession

/*
 * Uses Spark 2.0
 * Driver to initiate Nesting spark application locally.
 * Creates hive tables locally and performs insertions and selection
 * on them.
 */
object NestedDriver {

  def main(args: Array[String]): Unit = {

    val runLocal = (args.length == 1 && args(0).equals("runlocal"))
    var spark: SparkSession = null
    //val warehouseLocation = "/Users/ninad/local_database/"
    val singleNesting: NestingExamples = new NestingExamples()

    if (true) {
      spark = SparkSession
        .builder().master("yarn")
        .appName("NestedStructures")
        //.config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()

      spark.conf.set("spark.broadcast.compress", "false")
      spark.conf.set("spark.shuffle.compress", "false")
      spark.conf.set("spark.shuffle.spill.compress", "false")
    }

    //creating all data tables. Need to run just once
//    createAllDataTable(spark)

    singleNesting.insertCustomerNestingSql(spark)
    singleNesting.insertCustomerPayment(spark)
    singleNesting.insertTransactionApi(spark)
    singleNesting.selectShippingSql(spark)
    singleNesting.selectShippingApi(spark)

  }

  //Creates all data tables. Refer DataCreator class for structure details
  def createAllDataTable(spark: SparkSession) = {

    val dataCreator: DataCreator = new DataCreator()

    dataCreator.createcustomerDetails(spark)
    dataCreator.createSamplecustomerPayments(spark)
    dataCreator.createcustomerTransaction(spark)
    dataCreator.createTransactionDetails(spark)
    dataCreator.createShipmentDetails(spark)
    dataCreator.createTransactionNestingHive(spark)
    dataCreator.createcustomerNestingHive(spark)

  }
}
