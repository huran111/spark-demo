package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author:HuRan
 * @Description:
 * @Date: Created in 22:39 2018/6/9
 * @Modified By:
 */
public class ParallelizeCollection {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf();
            conf.setAppName("parallelizeCollection");
            conf.setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Integer> numbers= Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> parallelize = sc.parallelize(numbers,5);//5个分区
        int sum=parallelize.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        System.out.println("1到9的累加和:"+sum);
        sc.close();
    }
}
