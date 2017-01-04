package org.spark.nested

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

class MultiNesting extends Serializable{
  
  def insertTransactionNestingSpark(spark: SparkSession) = {

    import spark.implicits._

    //read all non-nested tables in to dataframes
    val customerTransaction = spark.sql("select * from customer_transactions")
    val transactionDetails = spark.sql("select * from transaction_details")
    val shipmentDetails = spark.sql("select * from SHIPMENT_DETAILS")

    //create keyed rdd for all the above dataframes
    val keyedCustomerTransaction = customerTransaction.rdd.keyBy { row => row(1).toString() }
    val keyedTransaction = transactionDetails.rdd.keyBy { row => row(0).toString() }
    val keyedShipment = shipmentDetails.rdd.keyBy { row => row(0).toString() }
    
    //co-group innermost tables (transaction item and shipment details) and re-structure 
    val cogroup1 = keyedTransaction.cogroup(keyedShipment).values.map {
       case (e1: Iterable[Row], e2: Iterable[Row]) => deTangleTupledRows(e1.head,e2)
    }
    val keyedCogroup = cogroup1.keyBy{ row => row._1.toString()}
    
    //co-group outer transaction and above transaction-details rdd
    val tmp = keyedCustomerTransaction.cogroup(keyedCogroup).values.map {
      case (e1: Iterable[Row], e2: Iterable[(Int, Int, Int, Int, BigDecimal, Int, BigDecimal, Seq[(Int, String, String, String)])]) => nest(e1, e2)
    }

    //implicitly convert into dataframe and insert into nested transaction table
    val cogroupedDF = tmp.toDF()
    cogroupedDF.createOrReplaceTempView("cogrouped")
    val nested_insert = "insert into table transaction_nested select * from cogrouped"
    spark.sql(nested_insert)
  }

  //restructuring co-grouped rdd to match target schema
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

  //restructuring co-grouped rdd to match target schema
  def deTangleTupledRows(e1: Row, e2: Iterable[Row]): (Int, Int, Int, Int, BigDecimal, Int, BigDecimal, Seq[Tuple4[Int, String, String, String]]) = {
    val seq = e2.map { row => (row(1).toString().toInt, row(2).toString(), row(3).toString(), row(4).toString()) }.toSeq
    return ((e1(1).toString().toInt,e1(0).toString().toInt, e1(2).toString().toInt , e1(3).toString().toInt, BigDecimal(e1(4).toString()),
      e1(5).toString().toInt, BigDecimal(e1(6).toString()),
      seq))
  }
  
  def selectTransactionTypeHive(spark: SparkSession) = {
    val selectQuery = "select distinct tDet.TRANSCATION_TYPE from transaction_nested lateral view explode(transaction_details) as tDet"
    spark.sql(selectQuery).show()
  }
  
  def selectShipmentIdHive(spark: SparkSession) = {
//    val selectQuery = "select * from transaction_nested"
    val selectQuery = "select S.SHIPMENT_ID from (select distinct tDet.SHIPMENT_DETAILS as SHIPMENT_DETAILS from transaction_nested lateral " + 
      "view explode(item_details) as tDet where customer_id = 2) t lateral view explode(t.SHIPMENT_DETAILS) as S"
    spark.sql(selectQuery).show()
  }
  
}