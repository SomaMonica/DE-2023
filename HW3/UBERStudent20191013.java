import java.io.IOException;
import java.lang.InterruptedException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.io.Serializable;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class UBERStudent20191013 implements Serializable{

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
	public static void main(String[] args) {
		if(args.length < 1) {
			System.err.println("Usage: UBERStudent20191013 <in-file> <out-file>");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession.builder()
						.appName("UBERStudent20191013")
						.getOrCreate();
		
		JavaRDD<String> ubers = spark.read().textFile(args[0]).javaRDD();
		
		//map
		JavaPairRDD<String, String> uber = ubers.mapToPair(new PairFunction<String, String, String>(){ // input, outputK, outputV
			public Tuple2<String, String> call(String s){
				StringTokenizer itr = new StringTokenizer(s, ",");
				String baseNum = itr.nextToken();
				String date = itr.nextToken();
				String vehicles = itr.nextToken();
				String trips = itr.nextToken();
				
				return new Tuple2(baseNum + "," + getDay(date), trips + "," + vehicles);
			}
		});
		
		JavaPairRDD<String, String> rslt = uber.reduceByKey(new Function2<String, String, String>(){
			public String call(String x, String y) {
				StringTokenizer xItr = new StringTokenizer(x, ","); 
				int xTrips = Integer.parseInt(xItr.nextToken());
				int xVehicles = Integer.parseInt(xItr.nextToken());
				
				StringTokenizer yItr = new StringTokenizer(y, ","); 
				int yTrips = Integer.parseInt(yItr.nextToken());
				int yVehicles = Integer.parseInt(yItr.nextToken());
				
				xTrips += yTrips;
				xVehicles += yVehicles;
				
				return xTrips + "," + xVehicles;
			}
		});
		
		rslt.saveAsTextFile(args[1]);
		spark.stop();
	}

}
