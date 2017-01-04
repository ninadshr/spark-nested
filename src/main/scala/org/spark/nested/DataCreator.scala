package org.spark.nested

import org.apache.spark.sql.SparkSession

class DataCreator {

  def main(args: Array[String]): Unit = {

    val runLocal = (args.length == 1 && args(0).equals("runlocal"))
    var spark: SparkSession = null
    val warehouseLocation = "/Users/ninad/local_database/"

    if (runLocal) {
      spark = SparkSession
        .builder().master("local")
        .appName("NestedStructures")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()

      spark.conf.set("spark.broadcast.compress", "false")
      spark.conf.set("spark.shuffle.compress", "false")
      spark.conf.set("spark.shuffle.spill.compress", "false")
    }

    createcustomerDetails(spark)
    createSamplecustomerPayments(spark)
    createcustomerTransaction(spark)
    createTransactionDetails(spark)
    createShipmentDetails(spark)
    createTransactionNestingHive(spark)
    createcustomerNestingHive(spark)

  }

  def createcustomerDetails(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists customer_details")
    spark.sql("create external table customer_details (customer_ID	LONG, NAME STRING, SUBSCRIPTION_TYPE STRING, STREET_ADDRESS STRING,	CITY STRING," +
      "STATE	STRING, COUNTRY	STRING, ZIPCODE STRING,	START_DATE	STRING, END_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/customer_ecomm/'")
  }

  def createSamplecustomerPayments(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists customer_payments")
    spark.sql("create table customer_payments (customer_ID	LONG, NAME	STRING, SUBSCRIPTION_TYPE	STRING, STREET_ADDRESS	STRING, CITY	STRING, STATE	STRING, COUNTRY	STRING, ZIPCODE	STRING, START_DATE	STRING, END_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/customer_payment/'")
  }

  def createcustomerTransaction(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists customer_transactions")
    spark.sql("create table customer_transactions (customer_ID	LONG, TRANSACTION_ID	LONG, PAYMENT_ID	LONG, TRANSCATION_TYPE	STRING, STATUS	STRING, INITIATION_DATE	STRING, END_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/customer_transaction/'")
  }

  def createTransactionDetails(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists transaction_details")
    spark.sql("create table transaction_details (TRANSAC_ITEM_ID LONG, TRANSACTION_ID	LONG, ITEM_ID	LONG, QUANTITY	INT, PRICE	DECIMAL(38,10), DISCOUNT	LONG, PAYABLE DECIMAL(38,10))" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/transaction_details/'")
  }

  def createShipmentDetails(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists SHIPMENT_DETAILS	")
    spark.sql("create table SHIPMENT_DETAILS (TRANSAC_ITEM_ID	LONG, SHIPMENT_ID	LONG, ADDRESS	STRING,SHIP_DATE	STRING, ARRIVAL_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/shipment_details/'")
  }

  def createcustomerNestingHive(spark: SparkSession) = {
    spark.sql("drop table if exists nested_customer	")
    val customer_nested_create = "create table nested_customer (customer_id int, name string, subscription_type string, " +
      "address array<struct<street_address:string,city:string,state:string,country:string,zipcode:string,start_date:string," +
      "end_date:string>>)"
    spark.sql(customer_nested_create)
  }

  def createTransactionNestingHive(spark: SparkSession) = {
    spark.sql("drop table if exists transaction_nested_create	")
    val transaction_nested_create = "create table transaction_nested_create (CUSTOMER_ID INT, TRANSACTION_ID INT, " +
      "transaction_details array<struct<TRANSCATION_TYPE:STRING,STATUS:STRING, INITIATION_DATE:STRING, END_DATE:STRING>>," +
      "item_details array<struct<TRANSAC_ITEM_ID:INT, ITEM_ID:INT, QUANTITY:INT, PRICE:DECIMAL(38,10), DISCOUNT:INT, PAYABLE:DECIMAL(38,10)," +
      "SHIPMENT_DETAILS: array<struct<SHIPMENT_ID:INT, ADDRESS:STRING, SHIP_DATE:STRING, ARRIVAL_DATE:STRING>>>>)"
    spark.sql(transaction_nested_create)
  }

}