import java.io.IOException;
import java.lang.InterruptedException;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
public class ReduceSideJoin{
	public static class ReduceSideJoinMapper extends Mapper<Object,Text,Text,Text>{
		boolean fileA = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			Text outputK = new Text();
			Text outputV = new Text();
			String joinKey = "";
			String o_value = "";
			if(fileA){
				int productId = Integer.parseInt(itr.nextToken().trim());
				int price = Integer.parseInt(itr.nextToken().trim());
				String categoryCode = itr.nextToken().trim();
				joinKey = categoryCode;
				o_value = "A," + productId + "," + price;
			}else{
				String categoryCode = itr.nextToken().trim();
				String description = itr.nextToken().trim();
				joinKey = categoryCode;
				o_value = "B," + description;
			}
			outputK.set(joinKey);
			outputV.set(o_value);
			context.write(outputK, outputV);
		}
		protected void setup(Context context) throws IOException, InterruptedException{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if(filename.indexOf("relation_a") != -1){
				fileA = true;
			}else{
				fileA = false;
			}
		}
	}
	public static class ReduceSideJoinReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Text reduce_key = new Text();
			Text reduce_rslt = new Text();
			String description = "";
			ArrayList<String> buffer = new ArrayList<String>();
			
			for(Text val : values){
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				String file_type = itr.nextToken().trim();
				if(file_type.equals("B")){
					description = itr.nextToken().trim();
				}else{
					if(description.length() == 0){
						buffer.add(val.toString());
					}else{
						String productId = itr.nextToken().trim();
						String price = itr.nextToken().trim();
						reduce_key.set(productId);
						reduce_rslt.set(price + " " + description);
						context.write(reduce_key, reduce_rslt);
					}
				}
			}
			
			for(int i=0; i<buffer.size(); i++){
				StringTokenizer itr = new StringTokenizer(buffer.get(i), ",");
				itr.nextToken();
				reduce_key.set(itr.nextToken().trim());
				reduce_rslt.set(itr.nextToken().trim() + " " + description);
				context.write(reduce_key, reduce_rslt);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2){
			System.err.println("Usage: ReduceSideJoin <in> <out>"); 
   			System.exit(2);
		}
		
		Job job = new Job(conf, "ReduceSideJoin");
		job.setJarByClass(ReduceSideJoin.class);
		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}	
