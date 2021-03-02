package upf.edu.BiGramsApp;

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
                .filter(tweet -> tweet.isOriginal())
                .filter(tweet -> tweet.getLanguage().equals(language));

        /*
        * Here we find the bigrams, by first splitting the text into words and formatting
        * it as a list, we first normalise the words, we changed the normalise from the
        * wordCount example, that we explain below. And with the normalised words we
        * extract all the bigrams that we later flat so we take them out of
        * the list<list<String>>
        */
        JavaRDD<List<String>> tweetsText = tweets
                .map(tweet -> tweet.getText())
                .flatMap(s -> get_bigrams(normalise(Arrays.asList((s.split("[ ]"))))).iterator());

        /*
        * Here we count the amount of times the same bigram appears. By calling reduceByKey
        * and assigning a value of 1 to a pair of the type <bigram, 1> then when the same bigram
        * appears twice then it will sum making it <bigram, 2> and so on.
        */
        JavaPairRDD<List<String>, Integer> Bigrams = tweetsText
                .mapToPair(bigram -> new Tuple2<>(bigram, 1))
                .reduceByKey((a,b) -> a+b);

        /*
        * Since the way JavaPairRDD is build in spark, there's no sortByValue method,
        * thus we must swap the keys and values with the .swap() method from Scala.Tuple2.
        * Then we just sort the values in descending
        */
        JavaPairRDD<Integer,List<String>> SortedBigrams = Bigrams
                .mapToPair(bigram -> bigram.swap())
                .sortByKey(false);

        /*
        * We then take the 10 first tweets of our sourted tweets rdd
        * and convert the list back to an rdd with sc.parallelize().
        */
        List<Tuple2<Integer, List<String>>> listTop10Bigrams = SortedBigrams.take(10);
        JavaRDD<Tuple2<Integer, List<String>>> top10Bigrams = sc.parallelize(listTop10Bigrams);

        top10Bigrams.saveAsTextFile(outputFile);
    }


    /*
    * Similar to the wordCount example normalise function, we just adapted it to take
    * and return a list of words representing each tweet's text and normalise each word
    * in it.
    */
    private static List<String> normalise(List<String> words) {
        List<String> normWords = new ArrayList<>();
        for (String word : words){
            normWords.add(word.trim().toLowerCase());
        }
        return normWords;
    }

    /*
    * This function is used to map each tweet's text to a list of list of words
    * each nested list formed by two words, the bigram. By looping through the text
    * until the penultimate word and adding to our collection of words.
    */
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
