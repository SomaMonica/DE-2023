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

public class Movie {
	public static MovieMapper extends Mapper<Object, Text, Text, IntWritable>{
		private Text outputKey = new Text();
		private IntWritable outputVal = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), "::");
			
			int id = Integer.parseInt(itr.nextToken().trim());
			String titleNyear = itr.nextToken().trim();
			String genres = itr.nextToken().trim();
			
			StringTokenizer itr2 = new StringTokenizer(genres, "|");
			while(itr2.hasMoreTokens()) {
				outputKey.set(itr2.nextToken());
				context.write(outputKey, outputVal);
			}
			
		}
	}
	public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputVal = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable v : values) {
				sum += v.get();
			}
			outputVal.set(sum);
			context.write(key, outputVal);
		}
	
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: Movie <in> <out>"); System.exit(2);
		}

		Job job = new Job(conf, "Movie");
		job.setJarByClass(Movie.class);
		job.setMapperClass(MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	}

}
