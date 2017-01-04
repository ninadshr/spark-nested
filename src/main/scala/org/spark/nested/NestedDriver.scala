package org.spark.nested

import org.apache.spark.sql.SparkSession

object NestedDriver {

  def main(args: Array[String]): Unit = {

    val runLocal = (args.length == 1 && args(0).equals("runlocal"))
    var spark: SparkSession = null
    val warehouseLocation = "/Users/ninad/local_database/"
    val singleNesting: SingleNesting = new SingleNesting()
    val multiNesting: MultiNesting = new MultiNesting()

    if (runLocal) {
      spark = SparkSession
        .builder().master("local[1]")
        .appName("NestedStructures")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()

      spark.conf.set("spark.broadcast.compress", "false")
      spark.conf.set("spark.shuffle.compress", "false")
      spark.conf.set("spark.shuffle.spill.compress", "false")
    }

    //Inserts data into customer table using spark.
    singleNesting.insertcustomerNestingSpark(spark)
    
    //Inserts data into customer table using hive.
    singleNesting.insertcustomerNestingHive(spark)
    
    //Inserts data into transactions table using spark.(Multi-level nesting)
    multiNesting.insertTransactionNestingSpark(spark)
    
    
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