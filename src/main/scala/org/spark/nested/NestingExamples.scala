package org.spark.nested

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField

class NestingExamples {
  //Inserts data into nested_customer_details table using spark sql
  //uses collect_list(struct()) construct to create array of struct
  def insertCustomerNestingSql(sparkSession: SparkSession) = {
    sparkSession.sql("insert into table nested_customer_details" +
      " select cd.customer_id, cd.name, cd.subscription_type, cd.street_address, cd.city," +
      " cd.state, cd.country, cd.zipcode, cd.start_date,cd.end_date," +
      " collect_list(struct(cp.customer_id, cp.payment_id, cp.payment_type, cp.card_number, cp.billing_address,cp.start_date, cp.end_date)) as payment_info" +
      " from customer_details  cd inner join customer_payments cp on cd.customer_id = cp.customer_id" +
      " group by cd.customer_id, cd.name, cd.subscription_type, cd.street_address, cd.city, cd.state," +
      " cd.country, cd.zipcode, cd.start_date,cd.end_date")
    sparkSession.table("nested_customer_details").show(2, false)
  }

  //Inserts data into nested_customer_details table using spark API
  //Uses ScalaNested to cogroup and embed dataframes into one another. 
  //@ref:ScalaNester
  def insertCustomerPayment(sparkSession: SparkSession) = {
    val nester: ScalaNester = new ScalaNester()
    val customerDetails = sparkSession.table("customer_details")
    val customerPayments = sparkSession.table("customer_payments")
    val nestedDS = nester.nest(customerDetails, customerPayments, "payment_info", Array("customer_id"), sparkSession)
    nestedDS.createOrReplaceTempView("nested_customer_details_df")
    sparkSession.sql("insert into table nested_customer_details select * from nested_customer_details_df")
    sparkSession.table("nested_customer_details").show(2,false)
  }

  //Inserts data into nested_transaction_details table using spark API
  //Uses ScalaNested to cogroup and embed dataframes into one another. 
  //@ref:ScalaNester
  def insertTransactionApi(sparkSession: SparkSession) = {
    val nester: ScalaNester = new ScalaNester()
    val customerTransaction = sparkSession.table("CUSTOMER_TRANSACTIONS")
    val transactionDetails = sparkSession.table("TRANSACTION_DETAILS")
    val shipmentDetails = sparkSession.table("SHIPMENT_DETAILS")

    val nestedTransaction =
      nester.nest(customerTransaction,
        nester.nest(transactionDetails, shipmentDetails, "SHIPMENT_DETAILS",
          Array("transac_item_id"), sparkSession), "TRANSACTION_DETAILS", Array("transaction_id"), sparkSession)

    nestedTransaction.createOrReplaceTempView("nested_transaction_details_df")
    sparkSession.sql("insert into table nested_transaction_details select * from nested_transaction_details_df")
    sparkSession.table("nested_transaction_details").show(5, false)
  }

  //uses spark sql to select data from nested_customer_details and nested_transaction_details
  //where users are from New York and have un-delivered packages
  def selectShippingSql(sparkSession: SparkSession) = {
    sparkSession.sql("SELECT S.TRANSAC_ITEM_ID AS TRANSAC_ITEM_ID,S.SHIPMENT_ID AS SHIPMENT_ID, S.SHIP_DATE AS SHIP_DATE,"+ 
      "S.ARRIVAL_DATE AS ARRIVAL_DATE FROM ( SELECT TDET.SHIPMENT_DETAILS AS SHIPMENT_DETAILS FROM" +
      " NESTED_TRANSACTION_DETAILS TN JOIN NESTED_CUSTOMER_DETAILS CD ON TN.CUSTOMER_ID = CD.CUSTOMER_ID LATERAL VIEW EXPLODE(TRANSACTION_DETAILS)" +
      " AS TDET WHERE trim(CD.CITY) = 'New York') AS T LATERAL VIEW EXPLODE(T.SHIPMENT_DETAILS) AS S WHERE S.ARRIVAL_DATE IS NULL").show()
  }

  //performs similar logic as above query but uses spark API to do the same
  def selectShippingApi(sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._

    val transactionNestedDF = sparkSession.table("nested_transaction_details")
    val customerDF = sparkSession.table("NESTED_CUSTOMER_DETAILS").where("CITY = 'New York'")

    val shippingDetails = transactionNestedDF.join(customerDF, "CUSTOMER_ID")
      .withColumn("TRANSACTION_DETAILS", explode($"TRANSACTION_DETAILS"))
      .withColumn("SHIPMENT_DETAILS", explode($"TRANSACTION_DETAILS.SHIPMENT_DETAILS"))
      .filter(isnull($"SHIPMENT_DETAILS.ARRIVAL_DATE"))
      .select($"SHIPMENT_DETAILS.TRANSAC_ITEM_ID",$"SHIPMENT_DETAILS.SHIPMENT_ID", $"SHIPMENT_DETAILS.SHIP_DATE",$"SHIPMENT_DETAILS.ARRIVAL_DATE")
      shippingDetails.show()
  }

}