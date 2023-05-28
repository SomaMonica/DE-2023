import java.lang.InterruptedException;
import java.util.*;
import java.io.*;
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
	public static class DoubleKey implements WritableComparable{
		String movieId = new String(); // join key = MovieID
		String tableName = new String(); // Movies.dat & Ratings.dat
		
		public DoubleKey(String movieId, String tableName) {
			super();
			this.movieId = movieId;
			this.tableName = tableName;
		}
		
		public DoubleKey() {}
		
		//WritableComparable interface methods
		public void readFields(DataInput in) throws IOException{
			movieId = in.readUTF();
			tableName = in.readUTF();
		}
		public void write(DataOutput out) throws IOException{
			out.writeUTF(movieId);
			out.writeUTF(tableName);
		}
		
		public int compareTo(Object o1) {
			DoubleKey d = (DoubleKey) o1;
			int ret = movieId.compareTo(d.movieId);
			if(ret != 0) return ret;
			return tableName.compareTo(d.tableName); //M --> R
		}
	}
	public static class FirstPartitioner extends Partitioner<DoubleKey, Text>{
		public int getPartition(DoubleKey key, Text value, int numPartition) {
			return key.movieId.hashCode()%numPartition;
		}
	}
	public static class FirstGroupingComparator extends WritableComparator{
		protected FirstGroupingComparator() {
			super(DoubleKey.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleKey d1 = (DoubleKey) w1;
			DoubleKey d2 = (DoubleKey) w2;
			
			return d1.movieId.compareTo(d2.movieId); // movieId만 가지고 비교 - 같은 value list에 넣음
		}
	}
	public static class CompositeKeyComparator extends WritableComparator{
		protected CompositeKeyComparator() {
			super(DoubleKey.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleKey d1 = (DoubleKey) w1;
			DoubleKey d2 = (DoubleKey) w2;
			
			int rslt = d1.movieId.compareTo(d2.movieId);
			if(rslt == 0) {
				rslt = d1.tableName.compareTo(d2.tableName);
			}
			return rslt;
		}
	}
	public static class JoinNAvgMapper extends Mapper<LongWritable, Text, DoubleKey, Text>{
		boolean isMovie = true;
		
		protected void setup(Context ctx) throws IOException, InterruptedException{
			String filename = ((FileSplit)ctx.getInputSplit()).getPath().getName();
			if(filename.indexOf("movies.dat") == -1) isMovie = false;
		}
		public boolean isFantasy(String genres) {
			if(genres.toLowerCase().contains("fantasy")) return true;
			else return false;
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] val = value.toSting().split("::"); 
 			if(isMovie) {
				if(isFantasy(val[2])) {
					DoubleKey outputK = new DoubleKey(val[0], "M");
					context.write(outputK, new Text(val[1])); // Assasins (1995)
				}
			}else {
				DoubleKey outputK = new DoubleKey(val[1], "R");
				context.write(outputK, new Text(val[2])); // 5
			}
		}
	}
	public static class JoinNAvgReducer extends Reducer<DoubleKey, Text, Text, DoubleWritable>{
		public static boolean isNumeric(String str) { 
			  try {  
			    Double.parseDouble(str);  
			    return true;
			  } catch(NumberFormatException e){  
			    return false;  
			  }  
			}
		public void reduce(DoubleKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int cnt = 0;
			double sum = 0;
			String movieTitle = "";
			for(Text val : values) {
				if(cnt == 0) { //최초 1회
					movieTitle = val.toString().trim();
					if(isNumeric(movieTitle)) { // this isn't Fantasy movie
						break; // out of for-each loop
					}
					cnt++;
				}else {
					sum += Double.parseDouble(val.toString().trim());
					cnt++;
				}
			}
			cnt -= 1;
			if(sum > 0) {
				context.write(new Text(movieTitle), new DoubleWritable(sum/cnt));
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
			if(o1.avgRating < o2.avgRating) return -1; 	
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
		job1.setMapperClass(JoinNAvgMapper.class);
		job1.setReducerClass(JoinNAvgReducer.class);
		
		job1.setMapOutputKeyClass(DoubleKey.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setPartitionerClass(FirstPartitioner.class);
		job1.setGroupingComparatorClass(FirstGroupingComparator.class);
		job1.setSortComparatorClass(CompositeKeyComparator.class);
		
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
