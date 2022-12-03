package Hadoop.HadoopApp;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SalesExamTaskApp {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
    	Path inputPath = new Path("hdfs://localhost:9000/input/sales.csv");
    	FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
    	
    	// create job config for each task with the respective output value, output path, mapper, reducer etc.
    	JobConf[] jobConfigs = setupJobs(conf, inputPath, hdfs);
    			
		// Run each job
		for(int i = 0; i < jobConfigs.length; i++) {
	    	RunningJob runningJob = JobClient.runJob(jobConfigs[i]);
	    	runningJob.waitForCompletion();
    		System.out.println("--------------- Job number " + i + " completed! ---------------");
		}
	}
	
	private static JobConf[] setupJobs(Configuration conf, Path inputPath, FileSystem hdfs) throws Exception {
		// JOB 1
		Path outputPathJobOne = new Path("hdfs://localhost:9000/output/taskone_paymentscount_bytype");
		JobConf jobOne = setupJob(conf, inputPath, outputPathJobOne, hdfs, IntWritable.class, SalesExamTaskOneMapper.class, SalesExamTaskOneReducer.class);
				
		// JOB 2
		Path outputPathJobTwo = new Path("hdfs://localhost:9000/output/tasktwo_salessum_byproduct");
		JobConf jobTwo = setupJob(conf, inputPath, outputPathJobTwo, hdfs, DoubleWritable.class, SalesExamTaskTwoMapper.class, 		SalesExamTaskTwoReducer.class);
				
		// JOB 3
		Path outputPathJobThree = new Path("hdfs://localhost:9000/output/taskthree_transactionscount_byday");
		JobConf jobThree = setupJob(conf, inputPath, outputPathJobThree, hdfs, IntWritable.class, SalesExamTaskThreeMapper.class, 		SalesExamTaskThreeReducer.class);
				
		JobConf[] jobConfigs = {jobOne, jobTwo, jobThree};
		return jobConfigs;
	}
	
	protected static <Out, M extends Mapper, R extends Reducer> JobConf setupJob(Configuration conf, Path inputPath, 
			Path outputPath, FileSystem hdfs, Class<Out> outputValueClass,
			Class<M> mapperClass, Class<R> reducerClass) throws Exception {
    	JobConf job = new JobConf(conf, SalesExamTaskApp.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(outputValueClass);
    	job.setMapperClass(mapperClass);
    	job.setReducerClass(reducerClass);
    	job.setOutputFormat(TextOutputFormat.class);
    	
    	FileInputFormat.setInputPaths(job, inputPath);
    	FileOutputFormat.setOutputPath(job, outputPath);
    	
    	if(hdfs.exists(outputPath)) {
    		hdfs.delete(outputPath, true);
    	}
    	
    	return job;
	}
}
