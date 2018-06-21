package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author:HuRan
 * @Description: TransformationOperator实战
 * @Date: Created in 10:56 2018/6/10
 * @Modified By:
 */
public class TransformationOperator {
    public static void main(String[] args) {
        // map();
        //filter();
        //flatMap();
        //groupByKey();
        //reduceByKey();
        //sortByKey();
        //join();
        cogroup();
    }

    /**
     * 集合元素乘以2
     */
    public static void map() {
        SparkConf conf = new SparkConf();
        conf.setAppName("map");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);
        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
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

    /**
     * 过滤集合中的偶数
     */
    public static void filter() {
        SparkConf conf = new SparkConf()
                .setAppName("filter")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        //并行化集合，创建初始RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(list);
        JavaRDD<Integer> filter = javaRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });
        //打印新的RDD
        filter.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }

    /**
     * 将文本行拆分为单词
     */
    private static void flatMap() {
        SparkConf conf = new SparkConf()
                .setAppName("flatMap")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> line = Arrays.asList("hello you", "hello me", "hello world");
        JavaRDD<String> javaRDD = sc.parallelize(line);
        //我们需要自己定义FlatMapFunction的第二个泛型的类型
        JavaRDD<String> flatMap = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        flatMap.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }

    /**
     * 将每个班级的成绩进行分组
     */
    public static void groupByKey() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //模拟集合
        List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<>("class1", 23)
                , new Tuple2<>("class2", 32), new Tuple2<>("calss3", 323), new Tuple2<>("class2", 322));
        //并行化集合
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scores);
        JavaPairRDD<String, Iterable<Integer>> groupRDD = rdd.groupByKey();
        //打印一下新的RDD
        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class:" + t._1);
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
            }
        });
        sc.close();
    }

    /**
     * 统计班级的总分
     */
    public static void reduceByKey() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //模拟集合
        List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<>("class1", 23)
                , new Tuple2<>("class2", 32), new Tuple2<>("calss3", 323), new Tuple2<>("class2", 322));
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scores);
        JavaPairRDD<String, Integer> pairRDD = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println(s._1 + ":" + s._2);
            }
        });
        sc.close();
    }

    /**
     * 将学生分数进行排序
     */
    public static void sortByKey() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //模拟集合
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<Integer, String>(23, "a"),
                new Tuple2<>(21, "b"),
                new Tuple2<>(43, "c")
        );
        //根据key进行排序 可以手动指定升序还是降序
        //其中元素内容还是一样的  就是顺序不同了
        JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(list);
        JavaPairRDD<Integer, String> key = rdd.sortByKey(false);
        key.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> s) throws Exception {
                System.out.println(s._1 + ":" + s._2);
            }
        });
        sc.close();
    }

    /**
     * join和cogroup案例，打印学生成绩
     */
    private static void join() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "a"),
                new Tuple2<>(2, "b"),
                new Tuple2<>(3, "c")
        );
        List<Tuple2<Integer, Integer>> score = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 123),
                new Tuple2<>(2, 133),
                new Tuple2<>(3, 222)
        );
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(score);
        //第一个泛型，是之前两个javaPairRDD的可以的类型
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = students.join(scores);
        join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> s) throws Exception {
                System.out.println("stuent id:" + s._1);
                System.out.println("student name:" + s._2._1);
                System.out.println("studet score:" + s._2._2);
                System.out.println("===============================");
            }
        });
    }

    /**
     * join和cogroup案例，打印学生成绩
     */
    private static void cogroup() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "a"),
                new Tuple2<>(2, "b"),
                new Tuple2<>(3, "c")
        );
        List<Tuple2<Integer, Integer>> score = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 123),
                new Tuple2<>(2, 133),
                new Tuple2<>(3, 222)
        );
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(score);
        //第一个泛型，是之前两个javaPairRDD的可以的类型  cogroup和join不同， join上的所有value，都给放到Iterable中去
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = students.cogroup(scores);
        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> s) throws Exception {
                System.out.println("stuent id:" + s._1);
                System.out.println("student name:" + s._2._1);
                System.out.println("studet score:" + s._2._2);
                System.out.println("===============================");
            }
        });
    }
}
