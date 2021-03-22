package upf.edu.storage;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.clearspring.analytics.util.Lists;
import twitter4j.Query;
import twitter4j.Status;
import upf.edu.model.HashTagCount;

import java.io.Serializable;
import java.util.*;

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {
  static AmazonDynamoDB client;
  static DynamoDB dynamoDB;
  static Table dynamoDBTable;
  static String tableName = "LSDS2020-TwitterHashtags";

  public DynamoHashTagRepository() {
    final String endpoint = "dynamodb.us-east-1.amazonaws.com";
    final String region = "us-east-1";

    client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region)
            ).withCredentials(new ProfileCredentialsProvider("upf"))
            .build();
    dynamoDB = new DynamoDB(client);
    dynamoDBTable = dynamoDB.getTable(tableName);
  }

//  @Override
//  public void write(Status tweet) {
//    String text = tweet.getText();
//    String[] words = text.split("\\s");
//    String lang = tweet.getLang();
//    Long tweetId = tweet.getId();
//
//    for (int i=0; i<words.length; i++){
//      if (words[i].startsWith("#")){
//        String hashtag = words[i];
//
//        Map<String, AttributeValue> keymap = new HashMap<>();
//        keymap.put(
//                "hashtag", new AttributeValue(hashtag)
//        );
//        keymap.put(
//                "language", new AttributeValue(lang)
//        );
//
//        HashMap<String, AttributeValueUpdate> update = new HashMap<>();
//
//        update.put(
//                "count", new AttributeValueUpdate(
//                        new AttributeValue().withN("1"), AttributeAction.ADD
//                )
//        );
//        update.put(
//                "tweet_id", new AttributeValueUpdate(
//                        new AttributeValue().withNS(tweetId.toString()), AttributeAction.ADD
//                )
//        );
//
//        client.updateItem(tableName, keymap, update);
//        System.out.println("Writing Hashtag: " + hashtag);
//      }
//    }
//
//  }

  @Override
  public void write(Status tweet) {
    String text = tweet.getText();
    String[] words = text.split("\\s");
    String lang = tweet.getLang();
    Long tweetId = tweet.getId();

    for (int i=0; i<words.length; i++){
      if (words[i].startsWith("#")) {
        String hashtag = words[i];
        Integer count = 0;
        //Look for if hashtag already in DB
        Map<String, String> nameMap = new HashMap<>();
        nameMap.put("#h", "Hashtag");

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(":h", hashtag);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#h = :h")
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;
        try {
          items = dynamoDBTable.query(querySpec);
          iterator = items.iterator();
          while (iterator.hasNext()) {
            count++;
          }
        }
        catch (Exception e) {
          System.out.println(hashtag + " was not found in the database.");
        }

        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Hashtag", hashtag)
                .withUpdateExpression("SET icount = icount + :val, lang = :l")
                .withValueMap(new ValueMap()
                        .withNumber(":val", count)
                        .withString(":l", lang)
                )
                .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
          System.out.println("Adding " + hashtag + "to the database...");
          UpdateItemOutcome outcome = dynamoDBTable.updateItem(updateItemSpec);
          System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
          System.err.println("Unable to update item: " + hashtag);
          System.err.println(e.getMessage());
        }
      }
    }
  }


  @Override
  public List<HashTagCount> readTop10(String lang) {
    Map<String, String> nameMap = new HashMap<>();
    nameMap.put(
            "#lang", "language"
    );
    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put(
            ":l", lang
    );
    ScanSpec scanSpec = new ScanSpec()
            .withFilterExpression("#lang = :l")
            .withValueMap(valueMap)
            .withNameMap(nameMap);
    ItemCollection<ScanOutcome> langHashtags = dynamoDBTable.scan(scanSpec);

    List<HashTagCount> top10 = Lists.newArrayList();
    Iterator hashtagIterator = langHashtags.iterator();
    while(hashtagIterator.hasNext()){
      Map<String, Object> hashtagMap = ((Item) hashtagIterator.next()).asMap();
      String hashtag = (String) hashtagMap.get("hashtag");
      String lan = (String) hashtagMap.get("language");
      Long count = (Long) hashtagMap.get("count");

      top10.add(new HashTagCount(hashtag, lan,count));
    }
    top10.sort(new hashtagComparator());
    //In case the list of tweets is shorter than 10 tweets
    int lowestCount = Math.min(top10.size(), 10);
    return top10.subList(0, lowestCount);
  }
}

class hashtagComparator implements  Comparator<HashTagCount>, Serializable{
  @Override
  public int compare(HashTagCount a, HashTagCount b){
    return Long.compare(b.count, a.count);
  }
}