package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Author:HuRan
 * @Description:RDD持久化
 * @Date: Created in 22:37 2018/6/11
 * @Modified By:
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("persist")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E://a.txt").cache();
        long startTime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println("cost" + (endTime - startTime) + " millisnecs");
        long startTime1 = System.currentTimeMillis();
        long count1 = lines.count();
        System.out.println(count);
        long endTime1 = System.currentTimeMillis();
        System.out.println("cost" + (endTime1 - startTime1) + " millisnecs");
        sc.close();
    }
}
