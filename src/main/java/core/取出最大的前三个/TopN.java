package core.取出最大的前三个;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import parquet.example.data.simple.IntegerValue;
import scala.Tuple2;

import java.util.List;

/**
 * @Author:HuRan
 * @Description: 取出最大的前三个数字
 * @Date: Created in 22:18 2018/6/13
 * @Modified By:
 */
public class TopN {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("top")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E://spark.txt");
        JavaPairRDD<Integer, String> pair = lines.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s), s);
            }
        });
        JavaPairRDD<Integer, String> sortPairs = pair.sortByKey(false);
        JavaRDD<Integer> sortNumbers = sortPairs.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> v1) throws Exception {
                return v1._1;
            }
        });
        List<Integer> take = sortNumbers.take(3);
        for(Integer i:take){
            System.out.println(i);
        }
        sc.close();
    }

}
