import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class Youtube {
	public String category;
	public double average;
	
	public Youtube(String category, double average) {
		this.category = category;
		this.average = average;
	}
	
	public String getCategory() {
		return this.category;
	}
	
	public double getAverage() {
		return this.average;
	}
}

public class YouTubeStudent20191013 {
	public static class AverageComparator implements Comparator<Youtube> {
		public int compare(Youtube x, Youtube y) {
			if ( x.average > y.average ) return 1;
			if ( x.average < y.average ) return -1;
			return 0;
		}
	}
	
	public static void insertQueue(PriorityQueue q, String category, double average, int topK) {
		Youtube head = (Youtube) q.peek();
		if ( q.size() < topK || head.average < average ) {
			Youtube tmp = new Youtube(category, average);
			q.add(tmp);
			
			if(q.size() > topK) q.remove();
		}
		
	}
	
	public static class YoutubeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			String c = "";
			String r = "";
			
			int i = 0;
			while(itr.hasMoreTokens()) {
				r = itr.nextToken();
				if(i == 3) {
					c = r;
				}
				
				i++;
			}
			
			
			context.write(new Text(c), new DoubleWritable(Double.valueOf(r)));
		}
	}
	
	public static class YoutubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new AverageComparator();
		private int topK;
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		       	throws IOException, InterruptedException {
			double sum = 0;
			int cnt = 0;
			
			for(DoubleWritable val : values) {
				sum += val.get();
				cnt++;
			}
			
			double average = sum / cnt;
			
			insertQueue(queue, key.toString(), average, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>( topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Youtube tmp = (Youtube) queue.remove();
				context.write(new Text(tmp.getCategory()), new DoubleWritable(tmp.getAverage()));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: YouTubeStudent20191013 <in> <out> <k>");
			System.exit(2);
		}
		conf.setInt("topK", Integer.valueOf(otherArgs[2]));
		Job job = new Job(conf, "YouTubeStudent20191013");
		job.setJarByClass(YouTubeStudent20191013.class);
		job.setMapperClass(YoutubeMapper.class);
		job.setReducerClass(YoutubeReducer.class);
		job.setNumReduceTasks(1);	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
