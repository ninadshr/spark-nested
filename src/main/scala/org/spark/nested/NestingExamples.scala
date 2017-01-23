package org.spark.nested

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

class NestingExamples {

  def insertCustomerNestingSql(spark: SparkSession) = {
    spark.sql("insert into table nested_customer" +
      " select cd.customer_id, cd.name, cd.subscription_type, cd.street_address, cd.city," +
      " cd.state, cd.country, cd.zipcode, cd.start_date,cd.end_date," +
      " collect_list(struct(cp.customer_id, cp.payment_id, cp.payment_type, cp.card_number, cp.billing_address,cp.start_date, cp.end_date)) as payment_info" +
      " from customer_details  cd join customer_payments cp on cd.customer_id = cp.customer_id" +
      " group by cd.customer_id, cd.name, cd.subscription_type, cd.street_address, cd.city, cd.state," +
      " cd.country, cd.zipcode, cd.start_date,cd.end_date")
    spark.table("nested_customer").show()
  }

  def insertCustomerPayment(spark: SparkSession) = {
    val nester: ScalaNester = new ScalaNester()
    val customerDetails = spark.table("customer_details")
    val customerPayments = spark.table("customer_payments")
    val nestedDS = nester.nest(customerDetails, customerPayments, "payment_info", Array("customer_id"), spark)
    nestedDS.createOrReplaceTempView("nested_customer_df")
    spark.sql("insert into table nested_customer select * from nested_customer_df")
    spark.table("nested_customer").show()
  }

  def insertTransactionApi(spark: SparkSession) = {
    val nester: ScalaNester = new ScalaNester()
    val customerTransaction = spark.table("CUSTOMER_TRANSACTIONS")
    val transactionDetails = spark.table("TRANSACTION_DETAILS")
    val shipmentDetails = spark.table("SHIPMENT_DETAILS")

    val nestedTransaction =
      nester.nest(customerTransaction,
        nester.nest(transactionDetails, shipmentDetails, "SHIPMENT_DETAILS",
          Array("transac_item_id"), spark), "TRANSACTION_DETAILS", Array("transaction_id"), spark)

    nestedTransaction.createOrReplaceTempView("TRANSACTION_NESTED_DF")
    spark.sql("insert into table transaction_nested select * from TRANSACTION_NESTED_DF")
    spark.table("transaction_nested").show()
  }

  def selectShippingSql(spark: SparkSession) = {
    spark.sql("SELECT S.SHIP_DATE AS SHIP_DATE FROM ( SELECT TDET.SHIPMENT_DETAILS AS SHIPMENT_DETAILS FROM" +
      " TRANSACTION_NESTED TN JOIN CUSTOMER_DETAILS CD ON TN.CUSTOMER_ID = CD.CUSTOMER_ID LATERAL VIEW EXPLODE(TRANSACTION_DETAILS)" +
      " AS TDET WHERE trim(CD.CITY) = 'New York') AS T LATERAL VIEW EXPLODE(T.SHIPMENT_DETAILS) AS S where S.ARRIVAL_DATE = ''").show()
  }

  def selectShippingApi(spark: SparkSession) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val transactionNestedDS = spark.table("TRANSACTION_NESTED")
    val customerDS = spark.table("CUSTOMER_DETAILS").where("CITY = 'New York'")

    transactionNestedDS.join(customerDS, "CUSTOMER_ID")
      .withColumn("TRANSACTION_DETAILS", explode($"TRANSACTION_DETAILS"))
      .withColumn("SHIPMENT_DETAILS", explode($"TRANSACTION_DETAILS.SHIPMENT_DETAILS"))
      .withColumn("ARRIVAL_DATE", length($"SHIPMENT_DETAILS.ARRIVAL_DATE"))
      .filter(x => x.getAs("ARRIVAL_DATE") == 0).select($"SHIPMENT_DETAILS.SHIP_DATE")
      .show()
  }

}