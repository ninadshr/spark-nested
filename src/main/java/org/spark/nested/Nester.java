package org.spark.nested;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
//import org.apache.spark.sql.types.ArrayType;

import scala.Serializable;
import scala.Tuple2;

public class Nester implements Serializable{

	  public Dataset<Row> nest(Dataset<Row> into, Dataset<Row> from, String nestedFieldName, String[] keyFieldNames) {
		  
		  ExtractFieldsFunction extractFieldsFunction = new ExtractFieldsFunction(keyFieldNames);
	        JavaPairRDD<List<Object>, Row> keyedIntoRDD = into.javaRDD().keyBy(extractFieldsFunction);
	        JavaPairRDD<List<Object>, Row> keyedFromRDD = from.javaRDD().keyBy(extractFieldsFunction);

	        NestFunction nestFunction = new NestFunction();
	        JavaRDD<Row> nestedRDD = keyedIntoRDD.cogroup(keyedFromRDD).values().map(nestFunction);
	        
	        StructType nestedSchema = into.schema().add(nestedFieldName, DataTypes.createArrayType(from.schema()));
	        
	        Dataset<Row> nested = into.sqlContext().createDataFrame(nestedRDD, nestedSchema);
	        
	        return nested;
	  }
	  
	  @SuppressWarnings("serial")
	    private class ExtractFieldsFunction implements Function<Row, List<Object>> {
	        private String[] fieldNames;
	        
	        public ExtractFieldsFunction(String[] fieldNames) {
	            this.fieldNames = fieldNames;
	        }
	        
	        @Override
	        public List<Object> call(Row row) throws Exception {
	            List<Object> values = new ArrayList<Object>();
	            
	            for (String fieldName : fieldNames) {
	                values.add(row.get(row.fieldIndex(fieldName)));
	            }
	            
	            return values;
	        }
	    }
	  
	  @SuppressWarnings("serial")
	    private class NestFunction implements Function<Tuple2<Iterable<Row>, Iterable<Row>>, Row> {
	        @Override
	        public Row call(Tuple2<Iterable<Row>, Iterable<Row>> cogrouped) throws Exception {
	            // There should only be one 'into' record per key
	            Row intoRow = cogrouped._1().iterator().next();
	            Iterable<Row> fromRows = cogrouped._2();
	            int intoRowNumFields = intoRow.size();
	            
	            Object[] nestedValues = new Object[intoRowNumFields + 1];
	            for (int i = 0; i < intoRowNumFields; i++) {
	                nestedValues[i] = intoRow.get(i);
	            }
	            nestedValues[intoRowNumFields] = fromRows;
	            
	            Row nested = RowFactory.create(nestedValues);
	            
	            return nested;
	        }
	    }
	  
	  
	  
}