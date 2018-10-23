package com.kamal.sparkTopN;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkTopNMostOccuredWords {

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Invalid Input");
			return;
		}
		/* Initialize the configuration of the spark program */
		SparkConf conf = new SparkConf().setAppName("TopNMostOccuredWords");
		/* Set configuration of the spark java context */
		JavaSparkContext context = new JavaSparkContext(conf);
		/* Read the input file */
		JavaRDD<String> lines = context.textFile(args[0]);
		/* Flatmap converts list of lines to list of words */
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		/*
		 * Filter to check for stop-words and getting count of each word via reduceByKey
		 */
		JavaPairRDD<String, Integer> counts = words
				.filter((word) -> !StopWordsUtil.stopWords.contains(word.toLowerCase()))
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		/*
		 * In order to sort by value, we need to reverse the pairs and then sort
		 * descending
		 */
		JavaPairRDD<Integer, String> reversedAndSorted = counts
				.mapToPair(word -> new Tuple2<Integer, String>(word._2, word._1)).sortByKey(false);
		/* Trim the output by the top N words */
		List<Tuple2<Integer, String>> trim = reversedAndSorted.take(Integer.valueOf(args[2]));
		/*
		 * Convert trimmed output to JavaPairRDD and output the result in the specified
		 * directory
		 */
		JavaPairRDD<Integer, String> output = context.parallelizePairs(trim);
		output.saveAsTextFile(args[1]);
		context.close();
	}
}
