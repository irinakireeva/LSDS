package upf.edu.storage;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.clearspring.analytics.util.Lists;
import twitter4j.Status;
import upf.edu.model.HashTagCount;

import java.io.Serializable;
import java.math.BigDecimal;
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

  @Override
  public void write(Status tweet) {
    String text = tweet.getText();
    String[] words = text.split("\\s");
    String lang = tweet.getLang();
    Long tweetID = tweet.getId();

    for (String word : words) {
      if (word.startsWith("#")) {
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Hashtag", word, "lang", lang)
                .withUpdateExpression("SET icount = if_not_exists(icount, :empty) + :val, " +
                        "tweetIDList = list_append(if_not_exists(tweetIDList, :empty_list), :tweetId)")
                .withValueMap(new ValueMap()
                        .withNumber(":val", 1)
                        .withNumber(":empty", -1)
                        .withList(":empty_list", new ArrayList<String>())
                        .withList(":tweetId",tweetID.toString())
                )
                .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
          dynamoDBTable.updateItem(updateItemSpec);
        } catch (Exception e) {
          System.err.println("Unable to update item: " + word);
          System.err.println(e.getMessage());
        }
      }
    }
  }


  @Override
  public List<HashTagCount> readTop10(String lang) {
    List<HashTagCount> top10 = Lists.newArrayList();

    Map<String, String> nameMap = new HashMap<>();
    nameMap.put("#l", "lang");

    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put(":lan", lang);

    QuerySpec querySpec = new QuerySpec()
            .withKeyConditionExpression("#l = :lan")
            .withNameMap(nameMap)
            .withValueMap(valueMap);

    ItemCollection<QueryOutcome> items;
    Iterator<Item> iterator;
    try {
      items = dynamoDBTable.query(querySpec);
      iterator = items.iterator();
      while (iterator.hasNext()) {
        Map<String, Object> attributes = iterator.next().asMap();
        String h = (String) attributes.get("Hashtag");
        String l = (String) attributes.get("lang");
        BigDecimal decCount = (BigDecimal) attributes.get("icount");
        Long cl = decCount.longValue();
        top10.add(new HashTagCount(h,l,cl));
      }
    }
    catch (Exception e) {
      System.err.println(e.toString());
      System.err.println("Could not read the table.");
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