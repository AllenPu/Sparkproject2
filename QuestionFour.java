import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class QuestionFour {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("QuestionFour").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile("hdfs://master:9000/user/root/input/access_log").repartition(3);
        JavaPairRDD<String,Integer> pair = file.mapToPair(s -> new Tuple2(s.split(" ")[0],1));
        JavaPairRDD<String,Integer> count = pair.reduceByKey((int1,int2) ->(int1+int2));
        JavaPairRDD<Integer,String> webcount = count.mapToPair(listen -> new Tuple2<>(listen._2,listen._1));
        JavaPairRDD<Integer,String> webcountsort = webcount.sortByKey(true);
        JavaPairRDD<Integer,String> webcountsort1 = webcount.sortByKey(false);
        JavaPairRDD<String,Integer> list =webcountsort1.mapToPair(listen -> new Tuple2<>(listen._2,listen._1));
        System.out.println("the IP " + list.first()._1 + " made access for  "+ list.first()._2+"times");
    }
}