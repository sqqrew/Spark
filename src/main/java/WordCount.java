import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Alexey Filyushin on 11/1/14.
 */
public class WordCount {

    private static final String APP_NAME = " WordCount";
    private static String pattern = "[\\,.\\*\\[\\]()!?\\-'`\"]";
    private static List<String> list;

    public static void main(String[] args){
        list = new ArrayList<String>();
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> file = context.textFile(args[0]);
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                list.add("words: "+s);
                return Arrays.asList(s.replaceAll(pattern,"").toLowerCase().split(" "));
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                list.add("pairs: "+s);
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                list.add(String.valueOf("counts: " + v1 +"-" +v2));
                return v1  + v2;
            }
        });

        counts.sortByKey().saveAsTextFile(args[1]);
        for (String s : list){
            System.out.println(s);
        }
    }
}
