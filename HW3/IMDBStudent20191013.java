import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.util.List;
import java.util.regex.Pattern;

public class IMDBStudent20191013 implements Serializable{

	public static void main(String[] args) {
		if(args.length != 2) {
			System.err.println("Usage: IMDBStudent20191013 <in_file> <out_file>");
			System.exit(1);
		}
		SparkSession spark = SparkSession.builder().appName("IMDBStudent20191013").getOrCreate();
		
		JavaRDD<String> movies = spark.read().textFile(args[0]).javaRDD(); 
		
		FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>(){ 
			public Iterator<String> call(String s){
				String movie[] = s.split("::");
				String genres = movie[2];
				return Arrays.asList(s.split("|")).iterator(); // genres
			}
		}; 
		JavaRDD<String> genres = movies.flatMap(fmf);
		
		PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>(){
			public Tuple2<String, Integer> call(String s){
				return new Tuple2(s, 1);
			}
		};
		JavaPairRDD<String, Integer> ones = genres.mapToPair(pf);
		
		Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>(){
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		};
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);
		
		counts.saveAsTextFile(args[1]);
		spark.stop();

	}

}
