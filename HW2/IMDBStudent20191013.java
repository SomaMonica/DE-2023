import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20191013 {
	public static class Movie{
		public String title;
		public double avgRating;
		
		public Movie(String title, double avgRating) {
			this.title = title;
			this.avgRating = avgRating;
		}
		
		public String getTitle() {
			return this.title;
		}
		public double getAvgRating() {
			return this.avgRating;
		}
		
	}
	public static class DoubleString implements WritableComparable{
		String movieId = new String(); // join key = MovieID
		String tableName = new String(); // Movies.dat & Ratings.dat

		public DoubleString() {}

		public DoubleString(String _movieId, String _tableName) {
			super();
			this.movieId = _movieId;
			this.tableName = _tableName;
		}
		
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
			DoubleString o = (DoubleString) o1;
			int ret = movieId.compareTo(o.movieId);
			if(ret != 0) return ret;
			return tableName.compareTo(o.tableName); //M --> R
		}
		public String toString(){
			return movieId + " " + tableName;
		}
	}
	public static class FirstPartitioner extends Partitioner<DoubleString, Text>{
		public int getPartition(DoubleString key, Text value, int numPartition) {
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
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString d1 = (DoubleString) w1;
			DoubleString d2 = (DoubleString) w2;
			
			int rslt = d1.movieId.compareTo(d2.movieId);
			if(0 == rslt) {
				rslt = d1.tableName.compareTo(d2.tableName);
			}
			return rslt;
		}
	}
	public static class TopKMovieMapper extends Mapper<Object, Text, DoubleString, Text>{
		boolean isMovie = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] val = value.toSting().split("::"); 
			DoubleString outputK = new DoubleString();
			Text outputV = new Text();
 			if(isMovie) {
				String movieId = val[0];
				String title = val[1];
				String genres = val[2];
				if(isFantasy(genres) == true) {
					outputK = new DoubleString(movieId, "Movies");
					outputV.set("Movies::" + title);
					context.write(outputK, outputV); // Movie:Assasins (1995)
				}
			}else {
				String movieId = val[1];
				String avgRating = val[2];
				outputK = new DoubleString(movieId, "Ratings");
				outputV.set("Ratings::" + avgRating);
				context.write(outputK, outputV); // Rating:5
			}
		}
				
		protected void setup(Context context) throws IOException, InterruptedException{
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			if(filename.indexOf("movies.dat") != -1) isMovie = true;
   			else isMovie = false;
		}
		public boolean isFantasy(String genres) {
			if(genres.toLowerCase().contains("fantasy")) return true;
			else return false;
			
		}
	}
	public static class TopKMovieReducer extends Reducer<DoubleKey, Text, Text, DoubleWritable>{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		
		public void reduce(DoubleKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int cnt = 0;
			double sum = 0;
			String movieTitle = "";
			
			for(Text val : values){
				String[] v = val.toString().split("::");
				String fileName = v[0];
				if(cnt == 0){ //최초 1회
					if(fileName.equals("Rating")){
						break;
					}
					movieTitle = v[1];
				}else{
					sum += Double.parseDouble(v[1]);
				}
				cnt++;
			}
			if(sum != 0){
				double avg = sum/(cnt-1);
				insertQueue(queue, movieTitle, avg, topK);
			}
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
	
	public static class MovieComparator implements Comparator<Movie>{

		@Override
		public int compare(Movie o1, Movie o2) {
			if(o1.avgRating > o2.avgRating) return 1; // avgRating 큰 순서대로 = 내림차순
			if(o1.avgRating < o2.avgRating) return -1; 	
			else return 0;
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
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 3){
			System.err.println("Usage: TopKAvg <in> <out>"); 
   			System.exit(2);
		}
		
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		
		Job job = new Job(conf, "IMDBStudent20191013");
		job.setJarByClass(IMDBStudent20191013.class);
		job.setMapperClass(TopKMovieMapper.class);
		job.setReducerClass(TopKMovieReducer.class);
		
		job.setMapOutputKeyClass(DoubleKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}
