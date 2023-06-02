package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkPubMed_2 {

    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println("Usage: SparkPubMed_2 <year> <input_file_path> <output_file_path>");
            System.exit(1);
        }

        String year = args[0];
        String inputPath = args[1];
        String outputPath = args[2];


        SparkConf conf = new SparkConf().setAppName("SparkPubMed_2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath,2).rdd().toJavaRDD();

        JavaPairRDD<String, Integer> counts = input
                .filter(line -> line.split("\t")[6].equals(year))
                .mapToPair(line -> new Tuple2<>(line.split("\t")[7], 1))
                .reduceByKey(Integer::sum);

        // Format the output as "key\tvalue" and save to file
        counts.map(pair -> pair._1() + "\t" + pair._2())
                .saveAsTextFile(outputPath);
        sc.stop();
    }
}