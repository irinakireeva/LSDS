package upf.edu.TwitterLanguageFilterApp.filter;

import java.io.*;
import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import upf.edu.TwitterLanguageFilterApp.parser.SimplifiedTweet;

public class FileLanguageFilter {
    private final String InputFile;
    private final String OutputFile;
    private JavaSparkContext sc;

    public FileLanguageFilter(String inputFile, String outputFile, JavaSparkContext sc) {
        InputFile = inputFile;
        OutputFile = outputFile;
        this.sc = sc;
    }

    public void filterLanguage(String language) throws IOException {
        JavaRDD<String> tweets = sc.textFile(this.InputFile);
        tweets = tweets.filter(tweet -> tweet.length()>2);
        JavaRDD<SimplifiedTweet> tweetParser = tweets.map(x -> SimplifiedTweet.fromJson(x));
        tweetParser = tweetParser.filter(tweet -> tweet.getLanguage().equals(language));
    }
}
