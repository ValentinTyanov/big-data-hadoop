package Hadoop.HadoopApp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SalesExamTaskThreeReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
		
		int sum = 0;
		
		// get the count sum of all transactions (values) done for each day in the CSV
		// e.g. if key is 1/1/09 and there are 5 transactions done on this day, sum will be 5
		while(values.hasNext()) {
			sum += values.next().get();
		}		
		
		output.collect(key, new IntWritable(sum));	
	}

}
