import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.util.List;
import java.util.regex.Pattern;

public final class WordCount_spark implements Serializable{
	public static void main(String[] args) throws Exception{
		if(args.length < 2) {
			System.err.println("Usage: WordCount <in-file> <out-file>");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession.builder()
										.appName("WordCount")
										.getOrCreate();
		
		JavaRDD<String> lines = spark.read().testFile(args[0]).javaRDD(); // read text file
		
		FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>(){ // FlatMapFunction = flatMap()의 parameter type
			public Iterator<String> call(String s){
				return Arrays.asList(s.split(" ")).iterator(); // split 
			}
		};		
		JavaRDD<String> words = lines.flatMap(fmf); //flatMap() = iterator 반환
		
		//<input, outputKey, outputVal>
		PairFunction<String, String,Integer> pf = new PairFunction<String, String, Integer>(){ // Tuple2<K,V> 반환 & PairRDD 생성 
			public Tuple2<String, Integer> call(String s){
				return new Tuple2(s, 1);
			}
		};		
		JavaPairRDD<String, Integer> ones = words.mapToPair(pf); // mapToPair()
		
		Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>(){ // <T1, T2, outputV>
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		};	
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2); // reduceByKey() =동일 키에 대해 같은 함수 (f2) 적용
		
		counts.saveAsTextFile(args[1]); // Action
		spark.stop();
	}
}
