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

public class Uber {
	public static String getDay(String d) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(d, "/");
		int month = Integer.parseInt(itr.nextToken().trim());
		int date = Integer.parseInt(itr.nextToken().trim());
		int year = Integer.parseInt(itr.nextToken().trim());
		
		Calendar c = Calendar.getInstance();
		c.set(year, month, date);
		String day = null;
		switch(c.get(c.DAY_OF_WEEK)) {
		case 1:
			day = "SUN";
			break;
		case 2:
			day = "MON";
			break;
		case 3:
			day = "TUE";
			break;
		case 4:
			day = "WED";
			break;
		case 5:
			day = "THR";
			break;
		case 6:
			day = "FRI";
			break;
		case 7:
			day = "SAT";
			break;
		}
		return day;
	}
	public static UberMapper extends Mapper<Object, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputVal = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			
			String region = itr.nextToken().trim();
			String date = itr.nextToken().trim();
			String vehicles = itr.nextToken().trim();
			String trips = itr.nextToken().trim();
			
			outputKey.set(region + "," + getDay(date));
			outputVal.set(trips + "," + vehicles);
			
			context.write(outputKey, outputVal);
		}
	}
	public static class UberReducer extends Reducer<Text, Text, Text, Text>{
		private Text outputVal = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int sumOfTrips = 0;
			int sumOfVehicles = 0;
			
			for(Text v : values) {
				StringTokenizer itr = new StringTokenizer(v.toString(), ",");
				int trips = Integer.parseInt(itr.nextToken().trim());
				int vehicles = Integer.parseInt(itr.nextToken().trim());
				sumOfTrips += trips;
				sumOfVehicles += vehicles;
			}
			outputVal.set(sumOfTrips + "," + sumOfVehicles);
			context.write(key, outputVal);
		}
	
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: Uber <in> <out>"); System.exit(2);
		}

		Job job = new Job(conf, "Uber");
		job.setJarByClass(Uber.class);
		job.setMapperClass(UberMapper.class);
		job.setReducerClass(UberReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	}

}
