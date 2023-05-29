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
	public static class DoubleKey implements WritableComparable{
		String movieId = new String(); // join key = MovieID
		String tableName = new String(); // Movies.dat & Ratings.dat

		public DoubleKey() {}

		public DoubleKey(String movieId, String tableName) {
			super();
			this.movieId = movieId;
			this.tableName = tableName;
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
			if(0 == rslt) {
				rslt = d1.tableName.compareTo(d2.tableName);
			}
			return rslt;
		}
	}
	public static class TopKMovieMapper extends Mapper<Object, Text, DoubleKey, Text>{
		boolean isMovie = true;
		
		protected void setup(Context context) throws IOException, InterruptedException{
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			if(filename.indexOf("movies.dat") != -1) isMovie = true;
   else isMovie = false;
		}
		public boolean isFantasy(String genres) {
			if(genres.toLowerCase().contains("fantasy")) return true;
			else return false;
		
			
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String val[] = value.toSting().split("::"); 
 			if(isMovie) {
				if(isFantasy(val[2])) {
					DoubleKey outputK = new DoubleKey(val[0], "M");
					context.write(outputK, new Text("Movie::" + val[1])); // Movie:Assasins (1995)
				}
			}else {
				DoubleKey outputK = new DoubleKey(val[1], "R");
				context.write(outputK, new Text("Rating::" + val[2])); // Rating:5
			}
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
	public static class Movie{
		public String title;
		public double avgRating;
		
		public Movie(String _title, double _avgRating) {
			this.title = _title;
			this.avgRating = _avgRating;
		}
		
		public String getTitle() {
			return this.title;
		}
		public double getAvgRating() {
			return this.avgRating;
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
