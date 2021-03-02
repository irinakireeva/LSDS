package upf.edu.MostRetweetedApp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.ExtendedParser.ExtendedSimplifiedTweet;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MostRetweetedApp {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("MostRetweetedApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> argsList = Arrays.asList(args);
        String outputFile = argsList.get(0);
        String inputFile = argsList.get(1);

        //Read textfile
        JavaRDD<String> lines = sc.textFile(inputFile);

        //Convert textfile to ExtendedSimplifiedFile
        JavaRDD<ExtendedSimplifiedTweet> tweets = lines
                .map(line -> ExtendedSimplifiedTweet.fromJson(line))
                .filter(line -> line.isPresent())
                .map(tweet -> tweet.get());

        //Stage 1
        //Find the most retweeted users
        //First find the original tweets

        JavaRDD<ExtendedSimplifiedTweet> orTweets = tweets
                .filter(tweet -> tweet.isOriginal());

        //Find retweets

        JavaRDD<ExtendedSimplifiedTweet> rtTweets = tweets
                .filter(tweet -> !tweet.isOriginal());






    }
}
