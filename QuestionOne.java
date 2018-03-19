import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class QuestionOne {

    public static void main(String[] args){
        long start = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setAppName("QuestionOne").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile("hdfs://master:9000/user/root/input/access_log").repartition(3);
        JavaRDD<String> url = file.flatMap(line -> Arrays.asList(line.split(" ")[6]).iterator());
        Map<String, Long> numurl = url.countByValue();
        //Map<String, Long> url1 = new HashMap<>();
        //url1.put("/assets/img/loading.gif was hit",url1.get("/assets/img/loading.gif"));
        for(Map.Entry<String, Long> e: numurl.entrySet()) {
            if (e.getKey().equals("/assets/img/loading.gif")) {
                System.out.println("The times of hit to website \"/assets/img/loading.gif\" was  " + e.getValue());
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("the running time of the program is "+(end-start));
        //System.out.println("The times of hit to website \"/assets/img/loading.gif\" was  " + numurl.get("/assets/img/loading.gif"));


    }
}