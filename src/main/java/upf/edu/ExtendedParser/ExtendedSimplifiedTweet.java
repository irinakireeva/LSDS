package upf.edu.ExtendedParser;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.Optional;

public class ExtendedSimplifiedTweet implements Serializable {
    private static JsonParser parser = new JsonParser();
    private final long tweetId; // the id of the tweet (’id’)
    private final String text; // the content of the tweet (’text’)
    private final long userId; // the user id (’user->id’)
    private final String userName; // the user name (’user’->’name’)
    private final long followersCount; // the number of followers (’user’->’followers_count’)

    private final String language; // the language of a tweet (’lang’)
    private final boolean isRetweeted; // is it a retweet? (the object ’retweeted_status’ exists?)
    private final Long retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
    private final Long retweetedTweetId; // [if retweeted] (’retweeted_status’->’id’)
    private final long timestampMs; // seconds from epoch (’timestamp_ms’)

    public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName,
                                   long followersCount, String language, boolean isRetweeted,
                                   Long retweetedUserId, long retweetedTweetId, long timestampMs) {

        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.followersCount = followersCount;
        this.language = language;
        this.retweetedUserId = retweetedUserId;
        this.isRetweeted = isRetweeted;
        this.timestampMs = timestampMs;
        this.retweetedTweetId = retweetedTweetId;
    }

    /**
     * Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
     * If parsing fails, for any reason, return an {@link Optional#empty()}
     *
     * @param jsonStr
     * @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
     */
    public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {
        JsonElement je = parser.parse(jsonStr);
        if(je.isJsonNull()) {
            return Optional.empty();
        }
        JsonObject jo = je.getAsJsonObject();

        long tweetId = 0;
        String text = null;
        long userId = 0;
        String userName = null;
        long followersCount = 0;
        String language = null;
        boolean isRetweeted = false;
        Long retweetedUserId = 0L;
        Long retweetedTweetId = 0L;
        long timestampMs = 0;

        if(jo.has("id")){
            tweetId = jo
                    .get("id")
                    .getAsLong();
        }else{
            return Optional.empty();
        }
        if (jo.has("text")){
            text = jo
               .get("text")
               .getAsString();
        }else {
            return Optional.empty();
        }
        if (jo.has("user")){
            JsonObject userObj = jo
                    .get("user")
                    .getAsJsonObject();
            if (userObj.has("id")){
                userId = userObj
                        .get("id")
                        .getAsLong();
            }
            if (userObj.has("name")){
                userName = userObj
                        .get("name")
                        .getAsString();
            }
            if (userObj.has("followers_count")){
                followersCount = userObj
                        .get("followers_count")
                        .getAsLong();
            }
        } else {
            return Optional.empty();
        }
        if  (jo.has("lang")){
            language = jo
                    .get("lang")
                    .getAsString();
        } else {
            return Optional.empty();
        }
        if (jo.has("retweeted")){
            if (jo.get("retweeted").getAsBoolean()) {
                isRetweeted = true;
                retweetedUserId = jo
                        .get("retweeted_status")
                        .getAsJsonObject()
                        .get("user")
                        .getAsJsonObject()
                        .get("id")
                        .getAsLong();
                retweetedTweetId = jo
                        .get("retweeted_status")
                        .getAsJsonObject()
                        .get("id")
                        .getAsLong();
            }
        }
        if(jo.has("timestamp_ms")){
            timestampMs = jo
                    .get("timestamp_ms")
                    .getAsLong();
        }else{
            return Optional.empty();
        }

        ExtendedSimplifiedTweet tweet = new ExtendedSimplifiedTweet(tweetId, text, userId, userName, followersCount,
                                                                    language,isRetweeted, retweetedUserId,
                                                                    retweetedTweetId, timestampMs);
        return Optional.of(tweet);
    }

    public String getLanguage() {
        return this.language;
    }
    public boolean isOriginal(){
        return !this.isRetweeted;
    }

    public String getText(){
        return this.text;
    }
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}