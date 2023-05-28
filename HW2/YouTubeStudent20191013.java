import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
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
import java.lang.NumberFormatException;

public class YouTubeStudent20191013 {
	
	public static class Youtube{
		public String category;
		public double avgRating;
		
		public Youtube(String _category, double _avgRating) {
			this.category = _category;
			this.avgRating = _avgRating;
		}
		public String toString() {
			return category + "," + avgRating;
		}
		public String getCategory() {
			return category;
		}
		public double getAvgRating() {
			return avgRating;
		}
		
	}
	
	public static class YoutubeComparator implements Comparator<Youtube>{

		@Override
		public int compare(Youtube o1, Youtube o2) {
			if(o1.avgRating > o2.avgRating) return 1; // avgRating 큰 순서대로 = 내림차순
			if(o1.avgRating < o2.avgRating) return -1; 	
			return 0;
		}
	}
	
	public static void insertQueue(PriorityQueue<Youtube> q, String category, double avgRating, int topK) {
		Youtube head = (Youtube) q.peek();
		if(q.size() < topK || head.avgRating < avgRating) {
			Youtube youtube = new Youtube(category, avgRating);
			q.add(youtube);
			if(q.size() > topK) q.remove();
		}
	}
	
	public static class YouTubeMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] val = value.toString().split("|");
			String category = val[3];
			String avgRating = val[6].trim();
			context.write(new Text(category), new DoubleWritable(Double.valueOf(avgRating)));
			
		}
		
	}
	
	public static class YouTubeReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			int cnt = 0;
			double sum = 0;
			for(DoubleWritable val : values) {
				sum += val.get();
				cnt++;
			}
			
			insertQueue(queue, key.toString(), sum/cnt, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK" , -1);
			queue = new PriorityQueue<Youtube> (topK, comp); //comp 기준으로 size가 topK인 queue 생성
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0) { //queue가 0될 때까지
				Youtube youtube = queue.remove();
				context.write(new Text(youtube.getCategory()), new DoubleWritable(youtube.getAvgRating())); 
			}
		}
	}
	
	public static void main(String[] args)  throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 3){
			System.err.println("Usage: YouTubeStudent20191013 <in> <out>"); 
   			System.exit(2);
		}
		
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		

		Job job = new Job(conf, "YouTubeStudent20191013");
		job.setJarByClass(YouTubeStudent20191013.class);
		job.setMapperClass(YouTubeMapper.class);
		job.setReducerClass(YouTubeReducer.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
