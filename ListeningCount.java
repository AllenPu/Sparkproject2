import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ListeningCount {
    public static void main(String[] args) {
        //SparkConf conf = new SparkConf().setAppName("ListeningCount").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("ListeningCount").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile("hdfs://master:9000/user/root/input/user_artists.dat").repartition(1);
        //JavaRDD<String> file = sc.textFile("input/user_artists.dat").repartition(1);
        String head = file.first();
        file = file.filter(row -> !row.equals(head));
        //       JavaRDD< String > file = sc.textFile("input/user_artists.dat").
        // mapPartitionsWithIndex((index, iter) -> {
        //           if (index == 0 && iter.hasNext()) {
        //              iter.next();
        //              if (iter.hasNext()) {JavaRDD<String> file = sc.textFile("hdfs://master:9000/user/root/input/user_artists.dat").repartition(1)
        //                 iter.next();
        //              }
        //          }
        //          return iter;
        //      }, true);;
        JavaPairRDD<String,Integer> pair = file.
                mapToPair(s -> new Tuple2(s.split("\t")[1],Integer.parseInt(s.split("\t")[2])));
        JavaPairRDD<String,Integer> count = pair.reduceByKey((int1,int2) ->(int1+int2));
        JavaPairRDD<Integer,String> listencount = count.mapToPair(ls -> new Tuple2<>(ls._2,ls._1));
        JavaPairRDD<Integer,String> listencountsort = listencount.sortByKey(true);
        JavaPairRDD<Integer,String> listencountsort1 = listencount.sortByKey(false);
        JavaPairRDD<String,Integer> list = listencountsort1.mapToPair(listen -> new Tuple2<>(listen._2,listen._1));
        list.foreach(num->System.out.println("Listening counts of Artists \""+num._1+"\" is "+ num._2));
        sc.close();

        }

    }

