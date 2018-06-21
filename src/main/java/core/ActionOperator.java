package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author:HuRan
 * @Description: action操作实战
 * @Date: Created in 16:55 2018/6/10
 * @Modified By:
 */
public class ActionOperator {
    public static void main(String[] args) {
        //reduce();
        //take();
        //saveFile();
        countByKey();
    }

    private static void reduce() {
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //有个一集合，里面有1到10 个数字，现在要对10个数字进行累加
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        //首先将第一个和第二个元素传入call方法进行计算，会获取一个结果，接着向下一个元素传入call方法，以此类推
        Integer reduce = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(reduce);
        sc.close();
    }

    private static void collection() {
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //有个一集合，里面有1到10 个数字，现在要对10个数字进行累加
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        //使用Map操作将集合中所有数字乘以2
        JavaRDD<Integer> javaRDD = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        //不用foreach action 操作，在远程集群上遍历rdd中的元素
        //而是使用Collection操作，将分布在远程集群上的double number RDD的数据拉取到本地
        //这种方式不建议使用，因为要走网络传输，还可能在rdd中数据量特别大的情况下发生OOM
        List<Integer> collect = javaRDD.collect();
        for (Integer num : collect) {
            System.out.println(num);
        }
        sc.close();
    }

    /*
    统计有多少个元素  前三个
     */
    public static void take() {
        SparkConf conf = new SparkConf()
                .setAppName("take")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberlist = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> numbers = sc.parallelize(numberlist);
        //统计有多少个元素  远程集群上获取RDD的数据 但是Collection是获取所有数据
        List<Integer> list1 = numbers.take(3);
        for (Integer a : list1) {
            System.out.println(a);
        }
    }

    /**
     * 保存到文件中
     */
    public static void saveFile() {
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //有个一集合，里面有1到10 个数字，现在要对10个数字进行累加
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        //使用Map操作将集合中所有数字乘以2
        JavaRDD<Integer> doubleNumber = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        doubleNumber.saveAsTextFile("E://a.txt");

    }

    /**
     *
     */
    public static void countByKey() {
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //有个一集合，里面有1到10 个数字，现在要对10个数字进行累加
        List<Tuple2<String, String>> list = Arrays.asList(new Tuple2<String, String>("calss1", "a")
                , new Tuple2<>("class2", "b")
                , new Tuple2<>("class3", "c"));
        JavaPairRDD<String, String> students = sc.parallelizePairs(list);
        //统计每个班级的学生人数 就是统计每个key对应的元素个数
        Map<String, Object> map = students.countByKey();
        for (Map.Entry<String, Object> studentCount : map.entrySet()) {
            System.out.println(studentCount.getKey() + ":" + studentCount.getValue());
        }

    }
}

