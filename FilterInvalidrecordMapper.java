package FilterInvalidrecord;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class FilterInvalidrecordMapper extends Mapper<LongWritable, Text, Text, Text> {
		
	Text Company;
	Text Product;
	Text Details;
	
	@Override
	public void setup(Context context) {
		Company = new Test();
		Details = new Test();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("|");
		
		Company.set(lineArray[0]));
		Product.set(lineArray[1]));
		Details.set(value);
		
		if ( Company != "NA" && Product != "NA" )
		{
			context.write(Company, Details);
		}
	}
}