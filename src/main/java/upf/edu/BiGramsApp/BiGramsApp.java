package upf.edu.BiGramsApp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.BiGramsApp.parser.ExtendedSimplifiedTweet;


import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class BiGramsApp {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("BiGramsApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String inputFile = argsList.get(2);

        //Read textfile
        JavaRDD<String> lines = sc.textFile(inputFile);

        //Convert textfile to ExtendedSimplifiedFile
        JavaRDD<ExtendedSimplifiedTweet> tweets = lines
                .map(line -> ExtendedSimplifiedTweet.fromJson(line))
                .filter(line -> line.isPresent())
                .map(tweet -> tweet.get());

        //Now we must filter for original tweets
        tweets = tweets
                .filter(tweet -> tweet.isOriginal());

        //Find the bigrams
        JavaRDD<List<String>> tweetsText = tweets
                .map(tweet -> tweet.getText())
                .flatMap(s -> get_bigrams(normalise(Arrays.asList((s.split("[ ]"))))).iterator());

        JavaPairRDD<List<String>, Integer> Bigrams = tweetsText
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a+b);

        JavaPairRDD<Integer,List<String>> SortedBigrams = Bigrams
                .mapToPair(bigram -> bigram.swap())
                .sortByKey(false);

        List<Tuple2<Integer, List<String>>> top10Bigrams = SortedBigrams.take(10);
        JavaRDD<Tuple2<Integer, List<String>>> Top10Bigrams = sc.parallelize(top10Bigrams);

        Top10Bigrams.saveAsTextFile(outputFile);
    }


    private static List<String> normalise(List<String> words) {
        List<String> normWords = new ArrayList<>();
        for (String word : words){
            normWords.add(word.trim().toLowerCase());
        }
        return normWords;
    }

    private static List<List<String>> get_bigrams(List<String> text) {
        List<List<String>> bigrams = new ArrayList<>();

        for (int i = 0; i < text.size() - 1; i++){
            List<String> bigram = new ArrayList<>();
            bigram.add(text.get(i));
            bigram.add(text.get(i+1));
            bigrams.add(bigram);
        }

        return bigrams;
    }

}
