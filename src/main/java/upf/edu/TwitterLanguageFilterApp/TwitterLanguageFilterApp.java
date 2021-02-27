package upf.edu.TwitterLanguageFilterApp;

import org.apache.log4j.BasicConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import upf.edu.TwitterLanguageFilterApp.parser.SimplifiedTweet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TwitterLanguageFilterApp {
    public static void main(String[] args) throws IOException {

        BasicConfigurator.configure();
        SparkConf conf = new SparkConf().setAppName("TwitterLanguageFilter");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("Hola?");
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String inputFile = argsList.get(2);
	    JavaRDD<String> lines = sc.textFile(inputFile);
	    lines = lines.filter(tweet->tweet.length()>2);
	    JavaRDD<SimplifiedTweet> tweets = lines.map(tweet->SimplifiedTweet.fromJson(tweet).get());
	    tweets = tweets.filter(tweet->tweet.getLanguage().equals(language));
	    tweets.saveAsTextFile(argsList.get(1));
    }
}
