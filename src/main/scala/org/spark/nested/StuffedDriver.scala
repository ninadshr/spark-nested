package org.spark.nested

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.RowFactory

object StuffedDriver {
  def main(args: Array[String]): Unit = {

    val runLocal = (args.length == 1 && args(0).equals("runlocal"))
    var spark: SparkSession = null
    val warehouseLocation = "/Users/ninad/local_database/"

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

    //    createcustomerDetails(spark)
    //    createSamplecustomerPayments(spark)
        createcustomerTransaction(spark)
    createTransactionDetails(spark)
    createShipmentDetails(spark)
    createTransactionNestingHive(spark)
    insertTransactionNestingSpark(spark)
    //    createcustomerNestingHive(spark)
    //    insertcustomerNestingSpark(spark)
    //    createcustomerNestingHive(spark)
    //    insertcustomerNestingHive(spark)
    //    insertcustomerNestingSpark(spark)
  }

  def createcustomerDetails(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists customer_details")
    spark.sql("create external table customer_details (customer_ID	LONG, NAME STRING, SUBSCRIPTION_TYPE STRING, STREET_ADDRESS STRING,	CITY STRING," +
      "STATE	STRING, COUNTRY	STRING, ZIPCODE STRING,	START_DATE	STRING, END_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/customer_ecomm/'")
    println("customer_details rows : " + spark.sql("select count(*) from customer_details").collect()(0)(0))
  }

  def createSamplecustomerPayments(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists customer_payments")
    spark.sql("create external table customer_payments (customer_ID	LONG, NAME	STRING, SUBSCRIPTION_TYPE	STRING, STREET_ADDRESS	STRING, CITY	STRING, STATE	STRING, COUNTRY	STRING, ZIPCODE	STRING, START_DATE	STRING, END_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/customer_payment/'")
    println("customer_payments rows : " + spark.sql("select count(*) from customer_payments").collect()(0)(0))
  }

  def createcustomerTransaction(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists customer_transactions")
    spark.sql("create external table customer_transactions (customer_ID	LONG, TRANSACTION_ID	LONG, PAYMENT_ID	LONG, TRANSCATION_TYPE	STRING, STATUS	STRING, INITIATION_DATE	STRING, END_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/customer_transaction/'")
    println("customer_transactions rows : " + spark.sql("select count(*) from customer_transactions").collect()(0)(0))
  }

  def createTransactionDetails(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists transaction_details")
    spark.sql("create external table transaction_details (TRANSAC_ITEM_ID LONG, TRANSACTION_ID	LONG, ITEM_ID	LONG, QUANTITY	INT, PRICE	DECIMAL(38,10), DISCOUNT	LONG, PAYABLE DECIMAL(38,10))" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/transaction_details/'")
    println("transaction_details rows : " + spark.sql("select count(*) from transaction_details").collect()(0)(0))
  }

  def createShipmentDetails(spark: org.apache.spark.sql.SparkSession) = {
    spark.sql("drop table if exists SHIPMENT_DETAILS	")
    spark.sql("create external table SHIPMENT_DETAILS (TRANSAC_ITEM_ID	LONG, SHIPMENT_ID	LONG, ADDRESS	STRING,SHIP_DATE	STRING, ARRIVAL_DATE STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/Users/ninad/Documents/customer_ecomm/shipment_details/'")
    println("SHIPMENT_DETAILS rows : " + spark.sql("select count(*) from SHIPMENT_DETAILS").collect()(0)(0))
  }

  def createcustomerTable(spark: org.apache.spark.sql.SparkSession) = {
    val customerDF = spark.read.option("hearder", true).csv("./target/classes/org/spark/nested/U.S._Chronic_Disease_Indicators__CDI_.csv")
    customerDF.createOrReplaceTempView("customer_view")
    spark.sql("drop table if exists customer")
    spark.sql("create table customer (YearStart String, YearEnd String, LocationAbbr String, " +
      "LocationDesc String, DataSource String, Topic String, Question String, Response String, " +
      "DataValueUnit String, DataValueTypeID String, DataValueType String, DataValue String, DataValueAlt " +
      "String, DataValueFootnoteSymbol String, DatavalueFootnote String, LowConfidenceLimit String, " +
      "HighConfidenceLimit String, StratificationCategory1 String, Stratification1 String, StratificationCategory2 " +
      "String, Stratification2 String, StratificationCategory3 String, Stratification3 String, GeoLocation String, " +
      "TopicID String, QuestionID String, ResponseID String, LocationID String, StratificationCategoryID1 String, StratificationID1 " +
      "String, StratificationCategoryID2 String, StratificationID2 String, StratificationCategoryID3 String, StratificationID3 String)" +
      " STORED AS PARQUET")
    spark.sql("insert into customer select * from customer_view")
    println(spark.sql("select count(*) from customer").collect()(0)(0))
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
//    spark.sql("select * from transaction_nested_create").schema.printTreeString()
  }

  def insertcustomerNestingHive(spark: SparkSession) = {

    val nested_insert = "insert into table nested_customer select customer_id, name, subscription_type, " +
      "collect_list(struct(street_address, city, state, country, zipcode, start_date,end_date))" +
      "from customer_details group by customer_id, name, subscription_type"
    spark.sql(nested_insert)
    spark.sql("select * from nested_customer").show
  }

  def insertcustomerNestingSpark(spark: SparkSession) = {

    import spark.implicits._

    val nestedCustomers = spark.sql("select * from customer_details").rdd.map { row =>
      (row(0).toString.toInt, row(1).toString(), row(2).toString(),
        Seq((row(3).toString(), row(4).toString(), row(5).toString(), row(6).toString(),
          row(7).toString(), row(8).toString(), row(9).toString())))
    }
    val nestedCustomersDF = nestedCustomers.toDF("id", "name", "subscription_type", "struct")
    nestedCustomersDF.printSchema()
    nestedCustomersDF.createOrReplaceTempView("nestedCustomers")

    val nested_insert = "insert into table nested_customer select * from nestedCustomers"
    spark.sql(nested_insert)
  }

  def insertTransactionNestingSpark(spark: SparkSession) = {

    import spark.implicits._

    val customerTransaction = spark.sql("select * from customer_transactions")
    val transactionDetails = spark.sql("select * from transaction_details")
    val shipmentDetails = spark.sql("select * from SHIPMENT_DETAILS")

    val keyedCustomerTransaction = customerTransaction.rdd.keyBy { row => row(1).toString() }
    val keyedTransaction = transactionDetails.rdd.keyBy { row => row(0).toString() }
    val keyedShipment = shipmentDetails.rdd.keyBy { row => row(0).toString() }
    
    val cogroup1 = keyedTransaction.cogroup(keyedShipment).values.map {
       case (e1: Iterable[Row], e2: Iterable[Row]) => deTangleTupledRows(e1.head,e2)
    }
    val keyedCogroup = cogroup1.keyBy{ row => row._1.toString()}
    val tmp = keyedCustomerTransaction.cogroup(keyedCogroup).values.map {
      case (e1: Iterable[Row], e2: Iterable[(Int, Int, Int, Int, BigDecimal, Int, BigDecimal, Seq[(Int, String, String, String)])]) => nest(e1, e2)
    }

    val cogroupedDF = tmp.toDF()
    cogroupedDF.printSchema()
    println("Here is my dataframe count " + cogroupedDF.count())
    cogroupedDF.createOrReplaceTempView("cogrouped")

    val nested_insert = "insert into table transaction_nested_create select * from cogrouped"
    spark.sql(nested_insert)
    spark.sql("select * from transaction_nested_create").show
  }

  def nest(e1: Iterable[Row], e2: Iterable[(Int, Int, Int, Int, BigDecimal, Int, BigDecimal, Seq[(Int, String, String, String)])]): 
      (Int, Int, Seq[Tuple4[String, String, String, String]], 
      Seq[Tuple7[Int, Int, Int, BigDecimal, Int, BigDecimal, Seq[Tuple4[Int,String, String, String]]]]) = {
    val rowTable1 = e1.seq.head //assuming only one row from table 1

    val deTangledSeq = e2.toSeq.map{ tuple => ((tuple._2.toString().toInt, tuple._3.toString().toInt, tuple._4.toString().toInt, BigDecimal(tuple._5.toString()),
      tuple._6.toString().toInt, BigDecimal(tuple._7.toString()),
      tuple._8))
      
    }

    return (rowTable1(0).toString().toInt, rowTable1(1).toString().toInt, Seq((rowTable1(2).toString(),
      rowTable1(3).toString(), rowTable1(4).toString(), rowTable1(5).toString())),
      deTangledSeq)

  }

  def deTangleTupledRows(e1: Row, e2: Iterable[Row]): (Int, Int, Int, Int, BigDecimal, Int, BigDecimal, Seq[Tuple4[Int, String, String, String]]) = {
    val seq = e2.map { row => (row(1).toString().toInt, row(2).toString(), row(3).toString(), row(4).toString()) }.toSeq
    return ((e1(1).toString().toInt,e1(0).toString().toInt, e1(2).toString().toInt , e1(3).toString().toInt, BigDecimal(e1(4).toString()),
      e1(5).toString().toInt, BigDecimal(e1(6).toString()),
      seq))
  }

}