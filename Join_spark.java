import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;


public class Join_spark implements Serializable{

	public static class Product{
		int productId;
		int price;
		String code;
		
		public Product(int productId, int price, String code) {
			this.productId = productId;
			this.price = price;
			this.code = code;
		}
		public Product() {	
		}
		
	}
	public static class Code{
		String code;
		String desc;
		public Code() {
		}
		public Code(String code, String desc) {
			this.code = code;
			this.desc = desc;
		}	
		
	}
	public static void main(String[] args) {
		if(args.length != 3) {
			System.err.println("Usage: Join_spark <in-file_a> <in-file_b> <out-file>");
			System.exit(2);
		}
		
		SparkSession spark = SparkSession.builder()
										.appName("Join_spark")
										.getOrCreate();
		
		JavaRDD<String> products = spark.read().testFile(args[0]).javaRDD(); // read relation_a
		
		PairFunction<String, String, Product> pfA = new PairFunction<String, String, Product>(){ // relation_a
			public Tuple2<String, Product> call(String s){
				StringTokenizer itr = new StringTokenizer(s, "|");
				String id = itr.nextToken();
				String price = itr.nextToken();
				String code = itr.nextToken(); // joinKey
					
				return new Tuple2(code, new Product(id, price, code));
			}
		};
		JavaPairRDD<String, Product> pTuples = products.mapToPair(pfA);
		
		JavaRDD<String> codes = spark.read().testFile(args[1]).javaRDD(); // read relation_b
		
		PairFunction<String, String, Code> pfB = new PairFunction<String, String, Code>(){
			public Tuple2<String, Code> call(String s){
				StringTokenizer itr = new StringTokenizer(s, "|");
				String code = itr.nextToken(); // joinKey
				String desc = itr.nextToken();
				
				return new Tuple2(code, new Code(code, desc));
			}
		};
		JavaPairRDD<String> cTuples = codes.mapToPair(pfB);
		
		JavaPairRDD<String, Tuple2<Product, Code>> joined = pTuples.join(cTuples);
		JavaPairRDD<String, Tuple2<Product, Optional<Code>>> leftOuterJoined = pTuples.leftOutperJoin(cTuples);
		JavaPairRDD<String, Tuple2<Optional<Product>, Code>> rightOuterJoined = pTuples.rightOuterJoin(cTuples);
		JavaPairRDD<String, Tuple2<Optional<Product>, Optional<Code>>> fullOuterJoined = pTuples.fullOuterJoin(cTuples);
		
		joined.saveAsTextFile(args[args.length - 1] + "_join");
		leftOuterJoined.saveAsTextFile(args[args.length - 1] + "_leftOuterJoin");
		rightOuterJoined.saveAsTextFile(args[args.length - 1] + "_rightOuterJoin");
		fullOuterJoined.saveAsTextFile(args[args.length - 1] + "_fullOuterJoin");
		
		spark.stop();
	}

}
