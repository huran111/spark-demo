package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

/**
 * @Author:HuRan
 * @Description: 统计每行出现的次数
 * @Date: Created in 23:21 2018/6/9
 * @Modified By:
 */
public class LineCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("linecount")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        //每个元素是一行文本
        JavaRDD<String> lines = sc.textFile("E://a.txt");
        JavaPairRDD<String, Integer> mapToPair = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });
        JavaPairRDD<String,Integer> linecounts=mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        linecounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+" append " +t._2 +" times.");
            }
        });
        sc.close();
    }
}
