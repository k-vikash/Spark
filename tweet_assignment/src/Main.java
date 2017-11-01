import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import scala.Tuple2;

import java.lang.String;
import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class Main {

	static class MyTupleComparator implements
			Comparator<Tuple2<String, Integer>>, Serializable {
		/**
		 * to compare the list for sorting
		 */
		final static MyTupleComparator INSTANCE = new MyTupleComparator();

		public int compare(Tuple2<String, Integer> t1,
				Tuple2<String, Integer> t2) {
			return -t1._2.compareTo(t2._2); // sorts RDD elements descending

		}
	}

	public static void main(String[] args) throws Exception {

        String inputFile = "tweets-1.txt";

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("Main").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> lines = sc.textFile(inputFile);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        
        JavaRDD<String> twitter_tags=words.filter(s -> s.contains("#"));
        JavaPairRDD<String, Integer> counts =
                twitter_tags.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                        .reduceByKey((x, y) -> x + y);

        // top 100 tag list with count
        List<Tuple2<String, Integer>> topNResult = counts.takeOrdered(100, MyTupleComparator.INSTANCE);

        for (Tuple2<?,?> tuple : topNResult) {
                System.out.println(tuple._1() + "   " + tuple._2());
        }
    }
}
