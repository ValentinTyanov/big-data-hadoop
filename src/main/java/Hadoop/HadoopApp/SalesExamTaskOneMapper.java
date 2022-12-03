package Hadoop.HadoopApp;

import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class SalesExamTaskOneMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	private final static HashSet<String> PAYMENT_TYPES = new HashSet<String>(Arrays.asList("Visa", "Mastercard", "Diners", "Amex"));
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		String csvRow = value.toString();
		String[] rowData = csvRow.split(",");
		
		String paymentType = rowData[3];
		
		// this means bad input has been provided in 2 possible ways:
		// Case 1: Unexisting payment type
		// Case 2: Index of payment type has been pushed further due to bad input (e.g. the "13,000" string for product price)
		// is bugging the splitter and this results in having both rowData[2] and [3] taken by the price ("13 and 000")
		if (!PAYMENT_TYPES.contains(paymentType)) {
			// if case 2, then paymentType is pushed to index 4, so check if that is the case
			paymentType = rowData[4];
			if (!PAYMENT_TYPES.contains(paymentType)) {
				throw new IOException("Unexisting payment type");
			}
		}
		
		// collect all occurrences of each paymentType from the csv
		output.collect(new Text(paymentType), new IntWritable(1));
	}
}
