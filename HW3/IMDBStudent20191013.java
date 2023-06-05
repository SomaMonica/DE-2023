import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import scala.Tuple2;

//spark
public class IMDBStudent20191013
{
        public static void main(String [] args) throws Exception
        {
		if(args.length < 1) {
		        System.out.println("Usage : IMDBStudent20191013 <in_file> <out_file>");
		        System.exit(1);
        	}

       SparkSession spark = SparkSession
			.builder()
			.appName("IMDBStudent20191013")
			.getOrCreate();
		
		JavaRDD<String> movies = spark.read().textFile(args[0]).javaRDD();
       
        FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>()
        {
                public Iterator<String> call(String s)
                {
                        ArrayList<String> result = new ArrayList<String>();
                        String [] splited = s.split("::");
                        StringTokenizer itr = new StringTokenizer(splited[2], "|");

                        while(itr.hasMoreTokens())
                        {
                                String genre = itr.nextToken();
                                result.add(genre);
                        }
                    	return result.iterator();
                }
        };

        JavaRDD<String> words = movies.flatMap(fmf);
        PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>()
        {
            public Tuple2<String, Integer> call (String s)
            {
                    return new Tuple2(s, 1);
            }
        };
        JavaPairRDD<String, Integer> ones = words.mapToPair(pf);
        
        Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>()
        {
            public Integer call(Integer x, Integer y)
            {
                    return x + y;
            }
        };

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);
        JavaRDD<String> resultRdd = counts.map(x -> x._1 + " " + x._2);
        
        // write result
        resultRdd.saveAsTextFile(args[1]);
        spark.stop();
    	}
}
