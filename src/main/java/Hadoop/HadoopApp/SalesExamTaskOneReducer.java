package Hadoop.HadoopApp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SalesExamTaskOneReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
		
		int sum = 0;
		
		// get the count sum of all occurrences of each payment type (e.g. in the CSV Mastercard has been used for 5 transactions)
		// => sum will be 5 for Mastercard
		while(values.hasNext()) {
			sum += values.next().get();
		}		
		
		output.collect(key, new IntWritable(sum));	
	}

}
