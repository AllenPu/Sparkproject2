import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class QuestionTwo {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("QuestionTwo").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile("hdfs://master:9000/user/root/input/access_log").repartition(3);
        JavaRDD<String> url = file.flatMap(line -> Arrays.asList(line.split(" ")[6]).iterator());
        Map<String, Long> numurl = url.countByValue();
        System.out.println("The time of hit to website was  " + numurl.get("/assets/js/lightbox.js"));
        for(Map.Entry<String, Long> e: numurl.entrySet()) {
            if (e.getKey().equals("/assets/js/lightbox.js")) {
                System.out.println("The times of hit to website \" / assets / js / lightbox.js\"\" was  " + e.getValue());
            }
        }
        sc.close();
    }
}


