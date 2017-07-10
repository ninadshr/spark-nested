#Spark Nesting Examples

This repo provides quick samples of how to handle complex types in Spark using both its API and SQL format. 

It contains a sample use case of customer transactions for typical online shopping model.
Creation scripts for below tables are included in the code (DataCreator.scala):

 - customer_details   
 - customer_payments
 - customer_transactions
 - transaction_details
 - shipment_details
 
Sample entries for these entities are also provided with the code base.

Code snippets in NestingExamples.scala has examples for:

- Inserting data for nested customer table with their payment information.
- Inserting data for nested transactions with shipping details
- Fetching data from above nested tables.

This application is written using Spark 2.0 with Scala 2.11. To setup and run this code on CDH cluster follow below steps:

**Step 1:** 
Upload all the csv data files included in src/main/resources on your hdfs gateway node. 

**Step 2:**
Run these commands from the node

<pre><code>
hdfs dfs -mkdir -p /data/customer_details
hdfs dfs -mkdir -p /data/customer_payments
hdfs dfs -mkdir -p /data/customer_transactions
hdfs dfs -mkdir -p /data/shipment_details
hdfs dfs -mkdir -p /data/transaction_details

hdfs dfs -put customer_details.csv /data/customer_details
hdfs dfs -put customer_payments.csv /data/customer_payments
hdfs dfs -put customer_transactions.csv /data/customer_transactions
hdfs dfs -put shipment_details.csv /data/shipment_details
hdfs dfs -put transaction_details.csv /data/transaction_details
</code></pre>

**Step 3:**
Compile this code using `mvn clean install` command. Upload the created jar in target directory on your Spark2 client gateway.

**Step 4:**
Sample command to run this code on CDH cluster

<pre><code>spark2-submit --class org.spark.nested.NestedDriver --master yarn spark-nested-1.0-SNAPSHOT.jar yarn</code></pre>

**To run locally**

 Keep all data file in some local path and point all table locations in DataCreator.scala class to these paths.
Once done simply compile code using `mvn clean install` and run with:
<pre><code>spark2-submit --class org.spark.nested.NestedDriver --master yarn spark-nested-1.0-SNAPSHOT.jar</code></pre>
