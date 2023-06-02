package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkPubMed_1 {

    public static void main(String[] args) {
        // check if input and output paths are provided
        if (args.length < 2) {
            System.err.println("Usage: SparkPubMed_1 <input_file_path> <output_file_path>");
            System.exit(1);
        }

        // create a SparkConf object and set the application name
        SparkConf conf = new SparkConf().setAppName("SparkPubMed_1").setMaster("local");

        // create a JavaSparkContext object
        JavaSparkContext sc = new JavaSparkContext(conf);


        // read the input text file as an RDD of lines
        JavaRDD<String> input = sc.textFile(args[0],2).rdd().toJavaRDD();

        // Split up into words.
        JavaPairRDD<String, Integer> yearCounts = input
                .map(line -> line.split("\t"))
                .filter(tokens -> tokens.length > 6)
                .mapToPair(tokens -> new Tuple2<>(tokens[6].trim(), 1));


        // Reduce by key to get the count of publications for each year
        JavaPairRDD<String, Integer> countsByYear = yearCounts
                .reduceByKey(Integer::sum);

        // Save the output to a text file
        countsByYear.map(tuple -> tuple._1 + "\t" + tuple._2)
                .saveAsTextFile(args[1]);


        // stop the Spark context
        sc.stop();
    }
}

