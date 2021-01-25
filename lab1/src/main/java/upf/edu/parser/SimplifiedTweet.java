package upf.edu.parser;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Optional;

public class SimplifiedTweet {

  private static JsonParser parser = new JsonParser();

  private final long tweetId;			  // the id of the tweet ('id')
  private final String text;  		      // the content of the tweet ('text')
  private final long userId;			  // the user id ('user->id')
  private final String userName;		  // the user name ('user'->'name')
  private final String language;          // the language of a tweet ('lang')
  private final long timestampMs;		  // seconds from epoch ('timestamp_ms')

  public SimplifiedTweet(long tweetId, String text, long userId, String userName,
                         String language, long timestampMs) {

    // PLACE YOUR CODE HERE!
    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.language = language;
    this.timestampMs = timestampMs;

  }

  /**
   * Returns a {@link SimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link SimplifiedTweet}
   */
  public static Object fromJson(String jsonStr) {

    // PLACE YOUR CODE HERE!
    JsonElement je = parser.parse(jsonStr);
    JsonObject jo  = je.getAsJsonObject();

    long tweetId;
    if (jo.has("id")){
      tweetId = jo.get("id")
              .getAsLong();

    } else {
      return Optional.empty();
    }

    String text = null;
    if (jo.has("text")){
      text = jo.get("text")
              .getAsString();

    } else {
      return Optional.empty();
    }

    String userName = null;
    long userId = Long.parseLong(null);
    if (jo.has("user")) {
      JsonObject userObj = jo.get("user")
              .getAsJsonObject();

      if (userObj.has("id")){
        userId = userObj.get("id")
                .getAsLong();
      }

      if (userObj.has("name")){
        userName = userObj.get("name")
                .getAsString();
      }

    } else {
      return Optional.empty();
    }

    String language;
    if (jo.has("lang")){
      language = jo.get("lang")
              .getAsString();
    } else {
      return Optional.empty();
    }

    long timestampMs;
    if (jo.has("timestamp_ms")){
      timestampMs = jo.get("timestamp_ms")
              .getAsLong();
    } else{
      return Optional.empty();
    }

    return new SimplifiedTweet(tweetId, text, userId, userName, language, timestampMs);

  }
  @Override
  public String toString() {
    //Overriding the way how SimplifiedTweets are printed in console or the output file
    new Gson().toJson(this);
  }
}
