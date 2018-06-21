package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @Author:HuRan
 * @Description:广播变量
 * @Date: Created in 23:04 2018/6/11
 * @Modified By:
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("broadcastVariable")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        int factor = 3;
        //使用共享变量
        final Broadcast<Integer> broadcast = sc.broadcast(factor);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        JavaRDD<Integer> map = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                int factor = broadcast.value();
                return v1 + factor;
            }
        });

        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }
}
