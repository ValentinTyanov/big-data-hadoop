package Hadoop.HadoopApp;

import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class SalesExamTaskThreeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		String csvRow = value.toString();
		String[] rowData = csvRow.split(",");
		
		String transactionDate = rowData[0];
		String[] dateFields;
		
		if (transactionDate.contains("/")) {
			dateFields = transactionDate.split("/");
		} else if (transactionDate.contains(".")) {
			dateFields = transactionDate.split(".");
		} else {
			throw new IOException("Bad date format. Accepted separators are / or .");
		}
		
		// m / d / yy
		String month = dateFields[0] + "/";
		String day = dateFields[1] + "/";
		//trim the hour and minute of transaction and keep just the year 
		String year = dateFields[2].substring(0, 2);
		
		String date = month + day + year;
		
		// collect all transactions made on each day provided in the CSV
		output.collect(new Text(date), new IntWritable(1));
	}
}
