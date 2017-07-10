Goto spark-nested folder on checkout and run "mvn clean install" command.
sftp spark-nested-1.0-SNAPSHOT.jar to your edge node (server running your hdfs gateway and spark client).

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