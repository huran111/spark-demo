package core.二次排序;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @Author:HuRan
 * @Description:
 * @Date: Created in 22:13 2018/6/12
 * @Modified By:
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sort")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://master:9000/user/date/2018-05-18/users.txt");
        JavaPairRDD<SecondSortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String[] lineSplit = line.split(" ");
                SecondSortKey key = new SecondSortKey(Integer.valueOf(lineSplit[0]), Integer.valueOf(lineSplit[1]));
             Tuple2<SecondSortKey,String> t=  new Tuple2<SecondSortKey, String>(key, line);
             System.out.println("Tuple2<SecondSortKey,String>:"+t._1.getSecond()+"  Tuple2<SecondSortKey,String>:"+t._2);
                System.out.println(line);
                return new Tuple2<SecondSortKey, String>(key, line);
            }
        });
        JavaPairRDD<SecondSortKey, String> sortPair = pairs.sortByKey();
        JavaRDD<String> map = sortPair.map(new Function<Tuple2<SecondSortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondSortKey, String> v1) throws Exception {
                System.out.println("v1._2:"+v1._2);
                System.out.println("v1._1:"+v1._1);

                return v1._2;
            }
        });
        map.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("******  :"+s);
            }
        });
        sc.close();
    }
}
