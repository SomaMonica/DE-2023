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
	
	public static class AvgMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		private Text outputKey = new Text();
		private DoubleWritable outputVal = new DoubleWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException, NumberFormatExceptin{
			String val[] = value.toString().split("|");
			outputKey.set(val[3]);
			outputVal.set(Double.parseDouble((val[6]).trim()));
			context.write(outputKey, outputVal);
		}
	}
	public static class AvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		private DoubleWritable outputVal = new DoubleWritable();

		int cnt = 0;
		double sum = 0;
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			for(DoubleWritable val : values) {
				sum += val.get();
				cnt++;
			}
			outputVal.set(sum/(double)cnt);
			context.write(key, outputVal);
		}
	}
	
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
	
	public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable>{
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			String category = itr.nextToken().trim();
			double avgRating = Double.parseDouble(itr.nextToken().trim());
			insertQueue(queue, category, avgRating, topK);
		}
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK" , -1);
			queue = new PriorityQueue<Youtube> (topK, comp); //comp 기준으로 size가 topK인 queue 생성
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0) { //queue가 0될 때까지
				Youtube youtube = (Youtube) queue.remove(); //head를 빼서
				context.write(new Text(youtube.toString()), NullWritable.get()); //emit
			}
		}
		
	}
	
	public static class TopKReducer extends Reducer<Text,NullWritable,Text,DoubleWritable>{
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(key.toString(), ","); // value가 아니라 key를 토큰화
			
			String category = itr.nextToken().trim();
			double avgRating = Double.parseDouble(itr.nextToken().trim());
			insertQueue(queue, category, avgRating, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK" , -1);
			queue = new PriorityQueue<Youtube> (topK, comp); //comp 기준으로 size가 topK인 queue 생성
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			//Text outputKey = new Text();
			//DoubleWritable outputVal = new DoubleWritable();
			while(queue.size() != 0) { //queue가 0될 때까지
				Youtube youtube = queue.remove();
				//outputKey.set(youtube.getCategory());
				//outputVal.set(youtube.getAvgRating());
				//context.write(outputKey, outputVal);
				context.write(new Text(youtube.getCategory()), new DoubleWritable(youtube.getAvgRating())); 
			}
		}
	}
	
	public static void main(String[] args)  throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 3){
			System.err.println("Usage: TopKAvg <in> <out>"); 
   			System.exit(1);
		}
		
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		
		String first_phase_result = "/first_phase_result";
		
		Job job1 = new Job(conf, "AvgOfRating");
		job1.setJarByClass(YouTubeStudent20191013.class);
		job1.setMapperClass(AvgMapper.class);
		job1.setReducerClass(AvgReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		FileSystem.get(job1.getConfiguration()).delete(new Path(first_phase_result), true);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
		
		Job job2 = new Job(conf, "TopKAvgOfRating");
		job2.setJarByClass(YouTubeStudent20191013.class);
		job2.setMapperClass(TopKMapper.class);
		job2.setReducerClass(TopKReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job2, new Path(first_phase_result));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		FileSystem.get(job2.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}

}
