package org.spark.nested

import org.apache.spark.sql.SparkSession

//Replace all path to paths of data files. See sample data in "src/main/resources"
class DataCreator {

  def createcustomerDetails(sparkSession: SparkSession) = {
    sparkSession.sql("DROP TABLE IF EXISTS CUSTOMER_DETAILS")
    sparkSession.sql("CREATE EXTERNAL TABLE CUSTOMER_DETAILS (CUSTOMER_ID INT, NAME STRING, SUBSCRIPTION_TYPE STRING, STREET_ADDRESS STRING,	CITY STRING," +
      "STATE	STRING, COUNTRY	STRING, ZIPCODE STRING,	START_DATE	TIMESTAMP, END_DATE TIMESTAMP)  " +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/customer_details/customer_details.csv'")
//      sparkSession.table("CUSTOMER_DETAILS").show()
  }

  def createSamplecustomerPayments(sparkSession: SparkSession) = {
    sparkSession.sql("DROP TABLE IF EXISTS CUSTOMER_PAYMENTS")
    sparkSession.sql("create external table customer_payments (CUSTOMER_ID INT,	PAYMENT_ID INT,	PAYMENT_TYPE STRING,	CARD_NUMBER STRING,	BILLING_ADDRESS	STRING, START_DATE TIMESTAMP,	END_DATE TIMESTAMP)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/customer_payments/customer_payments.csv'")
      sparkSession.table("CUSTOMER_PAYMENTS").show()
  }

  def createcustomerTransaction(sparkSession: SparkSession) = {
    sparkSession.sql("DROP TABLE IF EXISTS CUSTOMER_TRANSACTIONS")
    sparkSession.sql("CREATE EXTERNAL TABLE CUSTOMER_TRANSACTIONS (CUSTOMER_ID	INT, TRANSACTION_ID	INT, TRANSCATION_TYPE	STRING, STATUS	STRING, INITIATION_DATE	STRING, END_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/customer_transactions/customer_transactions.csv'")
//    sparkSession.table("CUSTOMER_TRANSACTIONS").show()
  }

  def createTransactionDetails(sparkSession: SparkSession) = {
    sparkSession.sql("DROP TABLE IF EXISTS TRANSACTION_DETAILS")
    sparkSession.sql("CREATE EXTERNAL TABLE TRANSACTION_DETAILS (TRANSAC_ITEM_ID INT, TRANSACTION_ID	INT, ITEM_ID	INT, QUANTITY	INT, PRICE	DECIMAL(38,10), DISCOUNT	INT, PAYABLE DECIMAL(38,10))" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/transaction_details/transaction_details.csv'")
//    sparkSession.table("TRANSACTION_DETAILS").show()
  }

  def createShipmentDetails(sparkSession: SparkSession) = {
    sparkSession.sql("DROP TABLE IF EXISTS SHIPMENT_DETAILS")
    sparkSession.sql("CREATE EXTERNAL TABLE SHIPMENT_DETAILS (TRANSAC_ITEM_ID	INT, SHIPMENT_ID	INT, ADDRESS	STRING,SHIP_DATE	TIMESTAMP, ARRIVAL_DATE TIMESTAMP)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/shipment_details/shipment_details.csv'")
//    sparkSession.table("SHIPMENT_DETAILS").show()
  }

  def createcustomerNestingHive(sparkSession: SparkSession) = {
    sparkSession.sql("DROP TABLE IF EXISTS NESTED_CUSTOMER_DETAILS")
    sparkSession.sql("CREATE TABLE NESTED_CUSTOMER_DETAILS (CUSTOMER_ID INT, NAME STRING, SUBSCRIPTION_TYPE STRING, STREET_ADDRESS STRING,	CITY STRING," +
      "STATE	STRING, COUNTRY	STRING, ZIPCODE STRING,	START_DATE	STRING, END_DATE STRING,  " +
      "PAYMENT_INFO ARRAY<STRUCT<customer_id:INT, payment_id:INT,	payment_type:STRING,	card_number:STRING,	billing_address:STRING, start_date:TIMESTAMP,	end_date:TIMESTAMP>>  " +
      " ) STORED AS PARQUET")
  }

  
  def createTransactionNestingHive(sparkSession: SparkSession) = {
    sparkSession.sql("DROP TABLE IF EXISTS NESTED_TRANSACTION_DETAILS")
    sparkSession.sql("CREATE TABLE NESTED_TRANSACTION_DETAILS (CUSTOMER_ID INT, TRANSACTION_ID INT, TRANSACTION_TYPE STRING, STATUS STRING, INITIATION_DATE TIMESTAMP, END_DATE TIMESTAMP, " +
      "TRANSACTION_DETAILS ARRAY<STRUCT<TRANSAC_ITEM_ID:INT, TRANSACTION_ID:INT, ITEM_ID:INT, QUANTITY:INT, PRICE:DECIMAL(38,10), DISCOUNT:INT, PAYABLE:DECIMAL(38,10)," +
      "SHIPMENT_DETAILS: ARRAY<STRUCT<TRANSAC_ITEM_ID:INT,SHIPMENT_ID:INT, ADDRESS:STRING, SHIP_DATE:TIMESTAMP, ARRIVAL_DATE:TIMESTAMP>>>>) STORED AS PARQUET")
  }

}
