package Hadoop.HadoopApp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SalesExamTaskTwoReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		
		double sum = 0;
		while(values.hasNext()) {
			// iterate over the collection of values for each key 
			// and add each value to the total sum
			sum += values.next().get();
		}

		// get the total sales sum for each product
		output.collect(key, new DoubleWritable(sum));
	}

}
