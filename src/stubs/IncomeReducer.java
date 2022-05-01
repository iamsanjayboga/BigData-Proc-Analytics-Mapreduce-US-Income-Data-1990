package stubs;

import java.io.IOException;

//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IncomeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

private  IntWritable iw = new IntWritable();
@Override
public void reduce(Text key, Iterable<IntWritable> values, Context context)
  throws IOException, InterruptedException {

		int sum = 0;
	
		for (IntWritable value : values) {
			
			  sum += value.get();
			 
		}
		
		iw.set(sum);
		context.write(key, iw);
	}
}