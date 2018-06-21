package core.取出最大的前三个;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Author:HuRan
 * @Description:
 * @Date: Created in 22:53 2018/6/13
 * @Modified By:
 */
public class GroupTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("top")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E://spark.txt");
        JavaPairRDD<String, Integer> pair = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<String, Integer>(split[0], Integer.valueOf(split[1]));
            }
        });
        List<Tuple2<String, Integer>> collect = pair.collect();
        for (Tuple2<String, Integer> s : collect) {
            System.out.println(s._1 + ":" + s._2);
        }
        System.out.println("------------------------");
        JavaPairRDD<String, Iterable<Integer>> groupByKey = pair.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> pair1 = groupByKey.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> s) throws Exception {
                Integer[] top3 = new Integer[3];
                String className = s._1;
                Iterator<Integer> iterator = s._2.iterator();
                while (iterator.hasNext()) {
                    Integer score = iterator.next();
                    for (int i = 0; i < 3; i++) {
                        if (top3[i] == null) {
                            top3[i] = score;
                            break;
                        } else if (score > top3[i]) {
                            int temp = top3[i];
                            top3[i] = score;
                            if (i < top3.length - 1) {
                                top3[i + 1] = temp;
                            }
                        } else {

                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });
        pair1.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> s) throws Exception {
                    System.out.println("calss"+s._1);
                Iterator<Integer> iterator = s._2.iterator();
                while (iterator.hasNext()){
                    System.out.println(iterator.next());
                }
                System.out.println("=================================");
            }
        });
    }
}
