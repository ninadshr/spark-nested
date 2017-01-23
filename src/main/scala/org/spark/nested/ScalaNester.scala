package org.spark.nested

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

class ScalaNester extends Serializable {

  def nest(into: Dataset[Row], from: Dataset[Row], nestedFieldName: String, keyFieldNames: Array[String], spark: SparkSession): Dataset[Row] = {
    //Keys the RDDs by key fields provided to the method   
    
    val keyedInto = into.rdd.keyBy { row =>
      extractKey(row, keyFieldNames)
    }
    val keyedFrom = from.rdd.keyBy { row =>
      extractKey(row, keyFieldNames)
    }
    val nested = keyedInto.cogroup(keyedFrom).values.map {
      e => Row.fromSeq(((for (i <- 0 until e._1.head.size) yield e._1.head(i)) :+ e._2))
    }
    spark.createDataFrame(nested, into.schema.add(nestedFieldName, DataTypes.createArrayType(from.schema)))
  }

  def extractKey(row: Row, keyFieldNames: Array[String]) = {
    for (i <- 0 until keyFieldNames.length) yield row(row.fieldIndex(keyFieldNames(i)))
  }

}
   
   	
