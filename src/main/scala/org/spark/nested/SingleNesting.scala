package org.spark.nested

import org.apache.spark.sql.SparkSession

class SingleNesting {
  
   def insertcustomerNestingHive(spark: SparkSession) = {

    val nested_insert = "insert into table nested_customer select customer_id, name, subscription_type, " +
      "collect_list(struct(street_address, city, state, country, zipcode, start_date,end_date))" +
      "from customer_details group by customer_id, name, subscription_type"
    spark.sql(nested_insert)
    spark.sql("select * from nested_customer").show
  }

  def insertcustomerNestingSpark(spark: SparkSession) = {

    import spark.implicits._

    //Re-structures customer table into nested table for natural encapsulation of data.
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
  
  def selectCustomerNestingHive(spark: SparkSession) = {
    val selectQuery = "select distinct customer_ts.street_address from nested_customer lateral view explode(address) " +
      "exploded_table as customer_ts where customer_id = 1 "
    spark.sql(selectQuery).show()
  }

}