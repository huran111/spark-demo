package core.二次排序.map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author:HuRan
 * @Description:
 * @Date: Created in 22:36 2018/6/13
 * @Modified By:
 */
public class mapTest {
    public static void main(String[] args) {
        map();
    }
    public static void flatMapToPairs(){

    }
    public static  void map(){
        SparkConf conf=new SparkConf()
                .setAppName("map")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E://spark.txt");
        JavaRDD<String[]> map = lines.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split(" ");
            }
        });
        List<String[]> collect = map.collect();
            for(String[] s:collect){
                for(String a:s){
                    System.out.println(a);
                }
            }
    }
    public void flatMap(){
        SparkConf conf=new SparkConf()
                .setAppName("map")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E://spark.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split);
            }
        });
        List<String> collect = words.collect();
        for(String s:collect){
            System.out.println(s);
        }
        sc.close();
    }
}
