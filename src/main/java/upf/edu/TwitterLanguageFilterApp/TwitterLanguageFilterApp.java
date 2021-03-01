package upf.edu.TwitterLanguageFilterApp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import upf.edu.TwitterLanguageFilterApp.parser.SimplifiedTweet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TwitterLanguageFilterApp {
    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setAppName("TwitterLanguageFilter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> argsList = Arrays.asList(args);
	String language = argsList.get(0);
	String outputFile = argsList.get(1);
	String inputFile = argsList.get(2);
        //Read the file in an RDD
		JavaRDD<String> lines = sc.textFile(inputFile);
		lines.filter( line -> line.length() > 2);

		//Now convert string files to SimplifiedTweet
		JavaRDD<SimplifiedTweet> tweetParser = lines.map(line -> SimplifiedTweet.fromJson(line).orElse(null));
		tweetParser = tweetParser.filter(tweet -> tweet!=null);
		tweetParser = tweetParser.filter(tweet -> tweet.getLanguage().equals(language));
		tweetParser.saveAsTextFile(outputFile);
        sc.close();
    }
}
