package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author:HuRan
 * @Description: 基于排序机制的wordcount
 * @Date: Created in 21:42 2018/6/12
 * @Modified By:
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sortwordcount")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E://spark.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //得到了每个单词出现的次数
        JavaPairRDD<String, Integer> wordcounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //wordcountsRDD内的元素是什么，（hello，3）（you，2）
        //内容反转
        JavaPairRDD<Integer, String> countWords = wordcounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> s) throws Exception {
                return new Tuple2<Integer, String>(s._2, s._1);
            }
        });
        //按照key进行排序
        JavaPairRDD<Integer, String> sordcountwords = countWords.sortByKey(false);
        //再次反转映射
        JavaPairRDD<String, Integer> sordwordscount = sordcountwords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> s) throws Exception {
                return new Tuple2<String, Integer>(s._2, s._1);
            }
        });
        sordwordscount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }


}
