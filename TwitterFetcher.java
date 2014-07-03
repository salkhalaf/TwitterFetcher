import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterFetcher {
	private static String consumerKey = "put your consumer key here";
	private static String consumerSecret = "put your consumer secret here";
	private static String accessToken = "put your access token here";
	private static String accessTokenSecret = "put your access token secret here";
	private static String screenName = "isale7";
	private static String dbName = "twitter_fetcher";
	private static DBCollection tweetsCollection;
	private static DB db;

	public static void main(String[] args) {
		setupDb();

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret)
		.setJSONStoreEnabled(true);
		TwitterFactory tf = new TwitterFactory(cb.build());

		Twitter twitter = tf.getInstance();

		List<Status> tweets = null;
		Status mostFavoured = null, mostRetweeted = null,  tweetByMostFollowers = null;
		int i = 1;
		String json;
		try {
			//when no more tweets available, will break out of the loop
			while(true)
			{
				//read the tweets in pages on a 100 tweets each
				tweets = twitter.getUserTimeline(screenName, new Paging(i++, 100));
				
				//no more tweets to process
				if(tweets.size() == 0)
					break;
				
				
				if(mostFavoured == null)
					mostFavoured = tweets.get(0);
				if(mostRetweeted == null)
					mostRetweeted = tweets.get(0);
				
				for (Status tweet : tweets) {
					
					//parse the json and store them in the db
					json = TwitterObjectFactory.getRawJSON(tweet);
					tweetsCollection.save( (DBObject) com.mongodb.util.JSON.parse(json));
					
					if(!tweet.isRetweet() && tweet.getFavoriteCount() > mostFavoured.getFavoriteCount())
						mostFavoured = tweet;
					if(!tweet.isRetweet() && tweet.getRetweetCount() > mostRetweeted.getRetweetCount())
						mostRetweeted = tweet;

					if(tweet.isRetweet() &&
							(tweetByMostFollowers == null || 
							tweet.getRetweetedStatus().getUser().getFollowersCount() > tweetByMostFollowers.getRetweetedStatus().getUser().getFollowersCount()))
						tweetByMostFollowers = tweet;
				}

			}
		} catch (TwitterException e) {
			e.printStackTrace();
			System.exit(0);
		}

		System.out.println("Total number of tweets is "+tweetsCollection.count());
		System.out.println("The most favored tweet is \""+mostFavoured.getText() + "\" was favored " + mostFavoured.getFavoriteCount() + " times");
		System.out.println("The most retweeted tweet is \""+mostRetweeted.getText() + "\" was retweeted " + mostRetweeted.getRetweetCount() + " times");
		System.out.println("The tweet with most famouse author is \""+tweetByMostFollowers.getText() + "\" the auther was " 
				+ tweetByMostFollowers.getRetweetedStatus().getUser().getScreenName() + " and s/he has " + tweetByMostFollowers.getRetweetedStatus().getUser().getFollowersCount() + " followers.");

		tweetsCollection.drop();


	}

	private static void setupDb() {
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(0);
		}

		db = mongoClient.getDB( dbName );
		tweetsCollection = db.getCollection("tweets");
	}

}
