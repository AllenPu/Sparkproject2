import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class newOneTwo {

    public static void main(String[] args){
        long start = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setAppName("NewOneTwo").setMaster("yarn");
        //SparkConf conf = new SparkConf().setAppName("NewOneTwo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile("hdfs://master:9000/user/root/input/access_log").repartition(3);
        //JavaRDD<String> file = sc.textFile("input/access_log").repartition(3);
        JavaRDD<String> url = file.flatMap(line -> Arrays.asList(line.split(" ")[6]).iterator());
        JavaRDD<String> url1 = url.filter(s -> s.contains("/assets/js/lightbox.js")) ;
        long a = url1.count();
        long intermid = System.currentTimeMillis();
        JavaRDD<String> url2 = url.filter(s -> s.contains("/assets/js/lightbox.js")) ;
        long b = url2.count();
        long end = System.currentTimeMillis();
        System.out.println("the first time of od rw is "+(intermid - start));
        System.out.println("the second time of od rw is "+(end - intermid));
        System.out.println("the first is nearly the "+(intermid - start)/(end - intermid)+" times of the second one excution");
    }
}