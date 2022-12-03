package Hadoop.HadoopApp;

import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SalesExamTaskTwoMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		
		String csvRow = value.toString();
		String[] rowData = csvRow.split(",");	
		
		// trim since Product3 has trailing space in 1 row
		String productType = rowData[1].trim();
		String productPrice = rowData[2];
		Double productPriceParsed;
		
		// using try-catch as one of the rows in the provided CSV file has product price
		// with bad format - ""13,000"" and those double double quotes crash the parsing
		try {
			productPriceParsed = Double.parseDouble(productPrice);
		} catch (NumberFormatException e) {
			System.out.println("--------------- Caught invalid format for product price: " + rowData[2] + " ---------------");
			System.out.println("--------------- Attempting to parse bad CSV input ---------------");
			
			String productPricePartOne = rowData[2].replaceAll("\"", "");
			// transform 000"" to 000
			String productPricePartTwo = rowData[3].replaceAll("\"", "");
			productPrice = productPricePartOne + productPricePartTwo;
			productPriceParsed = Double.parseDouble(productPrice);		
		}
	
		output.collect(new Text(productType), new DoubleWritable(productPriceParsed));
	}

}
