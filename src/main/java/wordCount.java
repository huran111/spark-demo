import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Author:HuRan
 * @Description:
 * @Date: Created in 21:40 2018/6/4
 * @Modified By:
 */
public class wordCount {
    public static void main(String[] args) {
        //创建sprakConf对象 设置spark应用的配置信息
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("wordcount");
        sparkConf.setMaster("local");//设置sparky应用程序的url设置为local代表本地运行
        //第二步创建javaSparkContext对象
        // 在spark中，SparkContext所有的一个入口
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /**
         *  第三步 针对输入源，创建一个初始的rdd
         *  输入源中的数据会打散，分配到rdd的每个 partition中，从而形成一个初始的
         *  分布式数据集
         *  sparkcontext中，用于根据文件类型的输入源创建rdd的 方法，叫做 textfile方法
         *  在java中，创建普通的Rdd,都叫做javaRDD
         *  如果function比较复杂 则会单独实现一个类 作为 这个function接口类的
         *
         *  现将一行拆分成单个单词
         *      flatmapFunction 两个泛型，分别代表了输入输出
         *
         */
        JavaRDD<String> lines = sc.textFile("E://spark.txt");
        lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(""));
            }
        });
        /**
         * 需要将每个单词映射为(word ,1)的格式
         * mapToPair其实就是将每个元素映射为一个(v1,v2)这样的Tuple2类型的元素
         * 第一个参数 代表了输入类型
         * 第二个参数和第三个参数，  代表了输出的第一个值 和第二个值的类型
         */
        JavaPairRDD<String, Integer> javaPairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        /**
         * 需要以单词作为key ，统计单词出现的次数
         * 比如 javaPairRDD中  有几个元素，分别为(hello,1),(hello,2)(word,3)
         * reduce操作 相当于就把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
         */
        JavaPairRDD<String, Integer> wordcouts = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        /**
         * 到这里为止，我么你通过spark算子，已经统计出了单词的次数
         * 但是，之前我们使用的flatMap,mapToPair,reduceByKey都是transformation操作
         * 最后要有一个action
         */
        wordcouts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1 + " append " + wordcount._2 + " times");
            }
        });
        //关闭
        sc.close();

    }
}
