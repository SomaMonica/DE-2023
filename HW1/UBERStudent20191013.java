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
import java.time.*;

public class UBERStudent20191013 {
	public static String getDay(String d) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(d, "/");
		int month = Integer.parseInt(itr.nextToken().trim());
		int date = Integer.parseInt(itr.nextToken().trim());
		int year = Integer.parseInt(itr.nextToken().trim());
		
		LocalDate ld = LocalDate.of(year, month, date);
	 	DayOfWeek dow = ld.getDayOfWeek();
  		
  		String rslt = null;
  		switch(dow.getValue()){
	  		case 1:
	  			rslt = "MON";
	  			break;
	  		case 2:
	  			rslt = "TUE";
	  			break;
	  		case 3:
	  			rslt = "WED";
	  			break;
	  		case 4:
	  			rslt = "THR";
	  			break;
	  		case 5:
	  			rslt = "FRI";
	  			break;
	  		case 6:
	  			rslt = "SAT";
	  			break;
	  		case 7:
	  			rslt = "SUN";
	  			break;
  		}
  		return rslt;
	}
	public static class UberMapper extends Mapper<Object, Text, Text, Text>{
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
		job.setJarByClass(UBERStudent20191013.class);
		job.setMapperClass(UberMapper.class);
		job.setReducerClass(UberReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
