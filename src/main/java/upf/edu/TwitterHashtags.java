package upf.edu;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

import java.io.IOException;

public class TwitterHashtags {

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);
        String tableName = "LSDS2020-TwitterHashtags";

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        //Acess the table
        final String endpoint = "dynamodb.us-east-1.amazonaws.com";
        final String region = "us-east-1";

        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, region)
                ).withCredentials(new ProfileCredentialsProvider("upf"))
                .build();
        final DynamoDB dynamoDB = new DynamoDB(client);
        final Table dynamoDBTable = dynamoDB.getTable(tableName);



        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
