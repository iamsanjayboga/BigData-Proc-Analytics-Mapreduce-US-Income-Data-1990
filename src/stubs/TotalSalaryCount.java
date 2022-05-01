package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class TotalSalaryCount {

	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		
		if (args.length != 2) {
			System.out.printf("Usage: TotalSalaryCount <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = Job.getInstance(conf, "Total Salary Count");
		job.setJarByClass(TotalSalaryCount.class);
		
		job.setMapperClass(IncomeMapper.class);
		job.setCombinerClass(IncomeReducer.class);
		job.setReducerClass(IncomeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	
	}
	
	
}
