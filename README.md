# spark-nested

This repo provides quick samples of how to handle complex types in Spark using bith its API and SQL format.

It contains a sample use case of customer transactions for typical online sopping model.
Entities containing:
 - customer_details
 - payment_info
 - transaction_details
 -shipping details
 
Sample entries for these entities is also provided with the code base.

Code snippets in NestingExamples.scala has examples for:
- Inserting data for nested customer table with their payment information.
- Inserting data for nested transactions with shipping details
- Fetching data from above nested tables.

The application is writte using Spark 2.0 with Scala 2.11
