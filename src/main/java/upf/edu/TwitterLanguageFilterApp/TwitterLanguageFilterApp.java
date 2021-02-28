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
        String inputFile = argsList.get(2);

        //Read the file in an RDD
	    JavaRDD<String> lines = sc.textFile(inputFile);

	    //Convert the JSON string lines to SimplifiedTweet
        JavaRDD<SimplifiedTweet> tweets = lines
                .map(line -> SimplifiedTweet.fromJson(line))
                .filter(line -> line.isPresent())
                .map(tweet -> tweet.get())
                .filter(tweet -> tweet.getLanguage().equals(language));

        tweets.saveAsTextFile(argsList.get(1));
        sc.close();
    }
}
