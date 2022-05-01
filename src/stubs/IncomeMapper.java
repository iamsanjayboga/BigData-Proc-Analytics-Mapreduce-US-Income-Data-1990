package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IncomeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//25, Private, 226802, 11th, 7, Never-married, Machine-op-inspct, Own-child, Black, Male, 0, 0, 40, United-States, <=50K.
		String data = value.toString();
		
		//String Inc = data[data.length-1]; //<=50K.
		//Inc = Inc.replace(".",""); //<=50K
		
		for (String word : data.split(",")) {
			
			if(word.length()>0){
				if(word.contains("<=50K") || word.contains(">50K")){
					context.write(new Text(word), new IntWritable(1));
				}
			}			
			
		}
		
	}
}