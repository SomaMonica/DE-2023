import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
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
public class IMDBStudent20191013 {
	
	public static boolean isFantasy(String genres) {
		StringTokenizer itr = new StringTokenizer(genres.trim(), "|");
		while(itr.hasMoreTokens()) {
			if((itr.nextToken().trim()).equals("Fantasy")) return true;
		}
		return false;
	}
	
	public static class AvgMapper extends Mapper<Object, Text, IntWritable, Text>{
		boolean isMovie = true;
		private IntWritable outputKey = new IntWritable();   
		private Text outputVal = new Text();
		
		protected void setup(Context context) throws IOException, InterruptedException{
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			if(filename.indexOf("movies.dat") != -1) {
				isMovie = true;
			}else {
				isMovie = false;
			}
		}
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String val[] = value.toString().split("::");
			if(isMovie) {
				if(isFantasy(val[2])) {
					outputKey.set(Integer.parseInt(val[0])); // Movie ID
					outputVal.set("M," + val[1]); // M,Title(year)
					context.write(outputKey, outputVal);
				}
			}else {
				outputKey.set(Integer.parseInt(val[1])); // Movie ID
				outputVal.set("R," + val[2]); // R,Rating
				context.write(outputKey, outputVal);
			}
		}
	}
	public static class AvgReducer extends Reducer<Text, Text, Text, DoubleWritable>{
		private Text outputKey = new Text();
		private DoubleWritable outputVal = new DoubleWritable();
		
		String movie_title = "";
		int cnt = 0;
		double sum = 0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				String file = itr.nextToken().trim();
				if(file.equals("M")) {
					movie_title = itr.nextToken().trim();
				}else {
					sum += Integer.parseInt(itr.nextToken().trim());
					cnt++;
				}
			}
			
			if(movie_title != "") {
				outputKey.set(movie_title);
				outputVal.set(sum/(double)cnt);
				context.write(outputKey, outputVal);
			}
		}
	}
	
	public static class Movie{
		public String title;
		public double avgRating;
		
		public Movie(String _title, double _avgRating) {
			this.title = _title;
			this.avgRating = _avgRating;
		}
		public String toString() {
			return title + "," + avgRating;
		}
		public String getTitle() {
			return title;
		}
		public double getAvgRating() {
			return avgRating;
		}
		
	}
	
	public static class MovieComparator implements Comparator<Movie>{

		@Override
		public int compare(Movie o1, Movie o2) {
			if(o1.avgRating > o2.avgRating) return 1; // avgRating 큰 순서대로 = 내림차순
			if(o1.avgRating > o2.avgRating) return -1; 	
			return 0;
		}
	}
	
	public static void insertQueue(PriorityQueue q, String title, double avgRating, int topK) {
		Movie head = (Movie) q.peek();
		if(q.size() < topK || head.avgRating < avgRating) {
			Movie movie = new Movie(title, avgRating);
			q.add(movie);
			if(q.size() > topK) q.remove();
		}
	}
	
	public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable>{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			String title = itr.nextToken().trim();
			double avgRating = Double.parseDouble(itr.nextToken().trim());
			insertQueue(queue, title, avgRating, topK);
		}
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK" , -1);
			queue = new PriorityQueue<Movie> (topK, comp); //comp 기준으로 size가 topK인 queue 생성
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0) { //queue가 0될 때까지
				Movie movie = (Movie) queue.remove(); //head를 빼서
				context.write(new Text(movie.toString()), NullWritable.get()); //emit
			}
		}
		
	}
	
	public static class TopKReducer extends Reducer<Text,NullWritable,Text,DoubleWritable>{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(key.toString(), ","); // value가 아니라 key를 토큰화
			
			String title = itr.nextToken().trim();
			double avgRating = Double.parseDouble(itr.nextToken().trim());
			insertQueue(queue, title, avgRating, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK" , -1);
			queue = new PriorityQueue<Movie> (topK, comp); //comp 기준으로 size가 topK인 queue 생성
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0) { //queue가 0될 때까지
				Movie movie = (Movie) queue.remove();
				context.write(new Text(movie.getTitle()), new DoubleWritable(movie.getAvgRating())); 
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 3){
			System.err.println("Usage: TopKAvg <in> <out>"); 
   			System.exit(3);
		}
		
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		
		String first_phase_result = "/first_phase_result";
		
		Job job1 = new Job(conf, "AvgOfRating");
		job1.setJarByClass(IMDBStudent20191013.class);
		job1.setMapperClass(AvgMapper.class);
		job1.setReducerClass(AvgReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		FileSystem.get(job1.getConfiguration()).delete(new Path(first_phase_result), true);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
		
		Job job2 = new Job(conf, "TopKAvgOfRating");
		job2.setJarByClass(IMDBStudent20191013.class);
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
