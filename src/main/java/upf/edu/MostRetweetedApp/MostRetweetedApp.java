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
import java.util.Comparator;
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

        //Find retweets

        JavaRDD<ExtendedSimplifiedTweet> rtTweets = tweets
                .filter(tweet -> !tweet.isOriginal());

        /*
        * pair retweets with the retweetedTweetId count how many times it appears
        * and then sorting in descending order
        */

        //Find the most retweeted users, the same way but taking retweeteduserid
        JavaPairRDD<Long, Integer> mostRetweetedUsersId = rtTweets
                .mapToPair(tweet -> new Tuple2<>(tweet.getRetweetedUserId(), 1))
                .reduceByKey((a, b) -> a+b)
                .mapToPair(tweetTuple -> tweetTuple.swap())
                .sortByKey(false)
                .mapToPair(tweetTuple -> tweetTuple.swap());


        /*
        * First we find the 10 most retweeted users, then we create an empty array
        * where we'll store the top tweet of each user. We obtain this by counting how
        * many times that tweet has been retweeted and then we order it to obtain the
        * largest one.*/
        List<Tuple2<Long, Integer>> top10Users = mostRetweetedUsersId.take(10);

        List<Long> topTweets = new ArrayList<>();

        for (int i = 0; i<top10Users.size(); i++){
            long user = top10Users.get(i)._1();
            
            JavaPairRDD<Long, Integer> userTopRetweets = tweets
                    .filter(tweet -> tweet.getRetweetedUserId() == user)
                    .mapToPair(tweet -> new Tuple2<>(tweet.getRetweetedTweetId(), 1))
                    .reduceByKey((a,b) -> a+b)
                    .mapToPair(tweet -> tweet.swap())
                    .sortByKey(false)
                    .mapToPair(tweet->tweet.swap());

            topTweets.add(userTopRetweets.take(1).get(0)._1());
        }

        JavaRDD<String> topTweets10TopUsers = tweets
                .filter(tweet -> topTweets.contains(tweet.getTweetId()))
                .map(tweet -> tweet.toString());

        topTweets10TopUsers.saveAsTextFile(outputFile);


    }
}
