package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SparkPubMed_3 {
    public static void main(String[] args) {

        // Create a SparkConf and JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("SparkPubMed_3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the TSV file into an RDD
        JavaRDD<String> rdd = sc.textFile(args[0],2);

        // Filter for authors from Australia and USA
        JavaRDD<String> filteredRdd = rdd.filter(line -> {
            String[] fields = line.split("\t");
            String country = fields[14];
            return country.equals("Australia") || country.equals("USA");
        });


        // Map each line to a tuple of (year, author_name)
        JavaRDD<Tuple2<String, Tuple2<String, String>>> tuplesRdd = filteredRdd.map(line -> {
            String[] fields = line.split("\t");
            String year = fields[6];
            String firstName = fields[3];
            String lastName = fields[2];
            return new Tuple2<>(year, new Tuple2<>(firstName, lastName));
        });

        // Reduce by key to count the number of publications for each (year, author_first_name, author_last_name) tuple
        JavaPairRDD<Tuple2<String, Tuple2<String, String>>, Integer> countsRdd = tuplesRdd
                .mapToPair(tuple -> new Tuple2<>(tuple, 1))
                .reduceByKey((a, b) -> a + b);

        // Map to a JavaPairRDD<String, Tuple2<String, Integer>>
        JavaPairRDD<String, Tuple2<String, Integer>> countTuplesRdd = countsRdd.mapToPair(tuple -> new Tuple2<>(tuple._1()._1(),
                new Tuple2<>(tuple._1()._2()._1() + " " + tuple._1()._2()._2(), tuple._2())));

        // Group by year and sort by number of publications in descending order
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> yearGroupsRdd = countTuplesRdd.groupByKey();
        JavaPairRDD<String, List<Tuple2<String, Integer>>> sortedRdd = yearGroupsRdd
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), StreamSupport.stream(tuple._2().spliterator(), false)
                        .sorted((a, b) -> b._2().compareTo(a._2()))
                        .limit(3)
                        .collect(Collectors.toList())));

        // Display the results

        sortedRdd.saveAsTextFile(args[1]);

        // Stop the JavaSparkContext
        sc.stop();
    }
}