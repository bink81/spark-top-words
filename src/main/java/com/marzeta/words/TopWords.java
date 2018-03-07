package com.marzeta.words;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Ordering;
import org.spark_project.guava.collect.SortedSetMultimap;
import org.spark_project.guava.collect.TreeMultimap;

import scala.Tuple2;

/**
 * Sample Spark application that shows top words in a given text file
 */
public final class TopWords {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopWords.class);

	private static SortedSetMultimap<Long, String> wordCount(final String filename) {
		try (JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("Top Words"))) {
			JavaRDD<String> input = sc.textFile(filename);

			// Split the input string into words (on any non-alphanumeric
			// characters)
			JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split("[^\\w']+")).iterator());

			// Transform the collection of words into pairs (word and 1) and
			// then add them up
			JavaPairRDD<String, Long> counts = words.mapToPair(s -> new Tuple2(s, (long) 1))
					.reduceByKey((x, y) -> (long) x + (long) y);
			return sortResults(counts);
		}
	}

	private static SortedSetMultimap<Long, String> sortResults(final JavaPairRDD<String, Long> counts) {
		List<Tuple2<String, Long>> listOfCounts = counts.collect();
		SortedSetMultimap<Long, String> results = TreeMultimap.create(Ordering.natural().reverse(), Ordering.natural());
		Pattern pattern = Pattern.compile(".*[a-z].*");
		for (Tuple2<String, Long> count : listOfCounts) {
			String s = count._1;
			Matcher matcher = pattern.matcher(s);
			if (!s.trim().isEmpty() && matcher.matches()) {
				results.put(count._2, count._1);
			}
		}
		return results;
	}

	private static void displayResults(final int maxPositions, final SortedSetMultimap<Long, String> myTreeMultimap) {
		int currentPosition = 0;
		for (Entry<Long, String> entry : myTreeMultimap.entries()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
			if (++currentPosition >= maxPositions) {
				break;
			}
		}
	}

	public static void main(final String[] args) {
		if (args.length == 0) {
			LOGGER.info("Usage: TopWords <file> [maxLimit]");
			System.exit(0);
		}
		SortedSetMultimap<Long, String> results = wordCount(args[0]);
		int maxPositions = determineDesiredNumberOfResults(args);
		System.out.println("Top " + maxPositions + " results (<OCCURENCES> : <VALUE>):");
		displayResults(maxPositions, results);
	}

	private static int determineDesiredNumberOfResults(final String[] args) {
		int maxPositions = 30;
		if (args.length == 2) {
			maxPositions = Integer.parseInt(args[1]);
		}
		return maxPositions;
	}
}
