package upf.edu.TwitterLanguageFilterApp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

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

        System.out.println("Language: " + language + ". Output file: " + outputFile);
        for (String inputFile : argsList.subList(2, argsList.size())) {
            System.out.println("Processing: " + inputFile);

        }
    }
}
