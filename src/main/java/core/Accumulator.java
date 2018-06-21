package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @Author:HuRan
 * @Description:累加变量
 * @Date: Created in 23:11 2018/6/11
 * @Modified By:
 */
public class Accumulator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("adfsdf")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建共享变量
        final org.apache.spark.Accumulator<Integer> accumulator = sc.accumulator(0);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        parallelize.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                accumulator.add(integer);
            }
        });
        sc.close();

    }
}
