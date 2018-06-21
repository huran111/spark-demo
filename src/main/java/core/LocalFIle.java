package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * @Author:HuRan
 * @Description: 使用本地文件创建RDD
 * @Date: Created in 23:02 2018/6/9
 * @Modified By:
 */
public class LocalFIle {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("LocalFile")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String>lines=sc.textFile("E://spark.txt");
        //统计文本的内容
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });
        Integer words = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("文件总次数："+words);
        sc.close();
    }
}
