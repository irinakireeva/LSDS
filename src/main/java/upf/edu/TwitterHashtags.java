package upf.edu;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.util.ConfigUtils;

import java.io.IOException;

public class TwitterHashtags {

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        String lang = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        DynamoHashTagRepository hashTagRepository = new DynamoHashTagRepository();
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        stream.window(Duration.apply(10000L)).foreachRDD(RDD -> RDD.foreach(tweet -> hashTagRepository.write(tweet)));

        System.out.println(hashTagRepository.readTop10(lang).toString());



        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
