package OpinionMining.Miner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

@SuppressWarnings("serial")
class Container implements Serializable {
	String sentiment;
	Double score;

	@Override
	public String toString() {
		return " " + sentiment + " " + score;
	}
}

@SuppressWarnings("serial")
class TweetContainer implements Serializable {
	String tweet;
	Double score;
	double latitude;
	double longitude;
	String placeName = "NONE";
	Map<String, Double> wordIntensity = new HashMap<>();

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{\"Tweet\":\"" + tweet + "\",\"ratings\":" + score + "");
		for (String word : wordIntensity.keySet()) {
			sb.append(word + " " + wordIntensity.get(word));
		}
		sb.append("\n");
		return sb.toString();
	}
}

public class DictionaryBuilder {

	private static final String STOP_WORD_PATH = "C:/Users/Mayur/Desktop/stopwords.csv";

	private static final String CONSUMER_KEY = "g3GCQbZJfiaGRSA3yMJEXFuus";

	private static final String CONSUMER_SECRET = "YVTciiKx1NNVcohdHFExtQo6ee6v35S3YDaP3g869iEDlUaFYd";

	private static final String ACCESS_TOKEN = "308582660-WOvXC9oihrwrsTLs6FYu55jb1NwaPBos5QRkSKAJ";

	private static final String ACCESS_TOKEN_SECRET = "ue5LqNe7EXcoDKOJuAOAtL6o6g8Y70mjeVGKZpctgIEei";

	static SparkConf sparkConf = new SparkConf().setAppName("OpinionMining").setMaster("local[8]");

	static JavaSparkContext context = new JavaSparkContext(sparkConf);

	static SQLContext sqlContext = new SQLContext(context);

	static File outF = new File("C:/Users/Mayur/workspace/bigdata-project/data.json");

	static BufferedWriter outFP = null;

	private static List<String> resultsfinalContainer = new ArrayList<>();

	private static Set<String> stopWords;

	/**
	 * @param args
	 * @throws IOException
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {

		System.setProperty("hadoop.home.dir", "C:\\jars\\");
		JavaRDD<String> file1 = context.textFile("C:/Users/Mayur/workspace/bigdata-project/SCL-OPP.txt");

		final Set<String> negativeEmotions = new HashSet<String>();
		negativeEmotions.add("anger");
		negativeEmotions.add("disgust");
		negativeEmotions.add("fear");
		negativeEmotions.add("negative");
		negativeEmotions.add("sadness");

		List<String> stpWords = context.textFile(STOP_WORD_PATH).flatMap(s -> Arrays.asList(s.split(","))).collect();

		stopWords = new HashSet<>(stpWords);

		//SCL-OPP dictionary Building
		JavaPairRDD<String, Double> oppDataSet = file1.mapToPair(new PairFunction<String, String, Double>() {

			public Tuple2<String, Double> call(String t) throws Exception {
				String[] str = t.split("\t");
				if (str[0].contains("#")) {
					str[0] = str[0].replace("#", "");
				}
				return new Tuple2<String, Double>(str[0], Double.parseDouble(str[1]));
			}
		});
		
		
		//Emotion dictionary Building        		
		JavaRDD<String> file2 = context.textFile(
				"C:/Users/Mayur/workspace/bigdata-project/NRC-Emotion-Lexicon-v0.92/NRC-emotion-lexicon-wordlevel-alphabetized-v0.92.txt");

		JavaRDD<String> items = file2.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) throws Exception {
				List<String> container = new ArrayList<String>();
				if (s.contains("1")) {
					container.add(s);
				}
				return container;
			}
		});

		JavaPairRDD<String, Container> filteredItems = items.mapToPair(new PairFunction<String, String, Container>() {
			public Tuple2<String, Container> call(String t) throws Exception {
				String str[] = t.split("\t");
				Container container = new Container();
				container.score = Double.parseDouble(str[2]);
				container.sentiment = str[1];
				if (negativeEmotions.contains(container.sentiment)) {
					container.sentiment = "negative";
					if (container.score > 0)
						container.score = -1 * container.score;
				}
				return new Tuple2<String, Container>(str[0], container);
			}
		});

		JavaPairRDD<String, Container> emotionDataSet = filteredItems
				.reduceByKey(new Function2<Container, Container, Container>() {

					public Container call(Container v1, Container v2) throws Exception {
						Container c1 = new Container();
						if (v1.score + v2.score < 0) {
							c1.sentiment = "negative";
						} else {
							c1.sentiment = "positive";
						}
						c1.score = v1.score + v2.score;
						return c1;
					}
				});

		JavaPairRDD<String, Double> emotionLexicon = emotionDataSet
				.mapToPair(new PairFunction<Tuple2<String, Container>, String, Double>() {
					public Tuple2<String, Double> call(Tuple2<String, Container> t) throws Exception {
						if (t._1.contains("angry")) {
							System.out.println("neg:" + t._2.score);
						}
						return new Tuple2<String, Double>(t._1, t._2.score);
					}
				});

		//Hashtag dictionary Building
		JavaRDD<String> file3 = context.textFile(
				"C:/Users/Mayur/workspace/bigdata-project/NRC-Hashtag-Emotion-Lexicon-v0.2/NRC-Hashtag-Emotion-Lexicon-v0.2.txt");

		JavaPairRDD<String, Container> filteredItems1 = file3.mapToPair(new PairFunction<String, String, Container>() {
			public Tuple2<String, Container> call(String t) throws Exception {
				String str[] = t.split("\t");
				Container container = new Container();
				container.score = Double.parseDouble(str[2]);
				container.sentiment = str[0];

				if (negativeEmotions.contains(container.sentiment)) {
					container.sentiment = "negative";
					if (container.score > 0)
						container.score = -1 * container.score;
				}

				return new Tuple2<String, Container>(str[1], container);
			}
		});

		JavaPairRDD<String, Container> hashTagData = filteredItems1
				.reduceByKey(new Function2<Container, Container, Container>() {
					public Container call(Container v1, Container v2) throws Exception {
						Container c1 = new Container();
						if (v1.score + v2.score < 0) {
							c1.sentiment = "negative";
						} else {
							c1.sentiment = "positive";
						}
						c1.score = v1.score + v2.score;
						return c1;
					}
				});

		JavaPairRDD<String, Double> hashTagLexicon = hashTagData
				.mapToPair(new PairFunction<Tuple2<String, Container>, String, Double>() {
					public Tuple2<String, Double> call(Tuple2<String, Container> t) throws Exception {
						return new Tuple2<String, Double>(t._1, t._2.score);
					}
				});

		//Affect Dictionary.
		JavaRDD<String> file4 = context.textFile("C:/Users/Mayur/workspace/bigdata-project/ddIntAff.csv");
		JavaPairRDD<String, Double> filteredItems3 = file4.mapToPair(new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String t) throws Exception {
				if (t.contains("weight")) {
					return new Tuple2<String, Double>("yyyy", 0.0);
				}
				String str[] = t.split(",");
				return new Tuple2<String, Double>(str[0], Double.parseDouble(str[3]));
			}
		});

		final Map<String, Double> hashTagMap = hashTagLexicon.collectAsMap();
		final Map<String, Double> oppMap = oppDataSet.collectAsMap();
		final Broadcast<Map<String, Double>> broadcast1 = context.broadcast(emotionLexicon.collectAsMap());
		final Broadcast<Map<String, Double>> broadcast2 = context.broadcast(hashTagMap);
		final Broadcast<Map<String, Double>> broadcast3 = context.broadcast(oppMap);
		final Broadcast<Map<String, Double>> broadcast4 = context.broadcast(filteredItems3.collectAsMap());

		long startTime = System.currentTimeMillis();

		/* set necessary properties */
		System.setProperty("hadoop.home.dir", "C:\\jars\\");
		System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY);
		System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET);
		System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN);
		System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET);

		/* twitter configuration */
		Configuration twitterConf = ConfigurationContext.getInstance();
		Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

		/* set-up java streaming context */
		JavaStreamingContext javaStream = new JavaStreamingContext(context, new Duration(10000));

		/* create twitter stream */
		TwitterUtils.createStream(javaStream);

		/* filters to retrieve tweets from */
		String[] filters = { "crystal palace" };

		/* retrieve filtered tweets */
		JavaReceiverInputDStream<Status> tweetStrem = TwitterUtils.createStream(javaStream, twitterAuth, filters);

		/* filter and clean all the tweets */
		tweetStrem.filter(tweet -> tweet.getLang().equals("en") && !tweet.getText().startsWith("RT"))
				.foreachRDD(rdd -> {
					System.out.println("RDD Count :" + rdd.count());
					processTweetStream(broadcast1, broadcast2, broadcast3, broadcast4, rdd);
				});

		javaStream.start();
		/* till the stream goes down or we terminate the job */
		javaStream.awaitTermination();

		long endTime = System.currentTimeMillis();
		System.out.println("Total time required in seconds is " + (endTime - startTime) / 1000);
	}

	/**
	 * method checks occurence of words in dictionary and assign sentiments  
	 * @param broadcast1
	 * @param broadcast2
	 * @param broadcast3
	 * @param broadcast4
	 * @param testData
	 * @throws IOException
	 */
	@SuppressWarnings("serial")
	private static void processTweetStream(Broadcast<Map<String, Double>> broadcast1,
			Broadcast<Map<String, Double>> broadcast2, Broadcast<Map<String, Double>> broadcast3,
			Broadcast<Map<String, Double>> broadcast4, JavaRDD<Status> testData) throws IOException {

		JavaRDD<TweetContainer> tweetResults = testData.map(new Function<Status, TweetContainer>() {

			public TweetContainer call(Status v1) throws Exception {

				List<String> str1 = tokenizeTweet(v1.getText());

				TweetContainer tweetContainer = new TweetContainer();
				tweetContainer.tweet = v1.getText();
				double score1 = 0.0;
				double score2 = 0.0;
				double score3 = 0.0;
				double score4 = 0.0;
				int pCount = 0;
				int nCount = 0;

				GeoLocation place = v1.getGeoLocation();

				if (place != null) {
					tweetContainer.placeName = place.getLatitude() + " " + place.getLongitude();
				}

				for (String str : str1) {
					if (broadcast1.getValue().containsKey(str)) {
						Double val = broadcast1.getValue().get(str);
						score1 += val.doubleValue();
					}

					if (broadcast2.getValue().containsKey(str)) {
						Double val = broadcast2.getValue().get(str);
						score2 += val.doubleValue();
					}

					if (broadcast3.getValue().containsKey(str)) {
						Double val = broadcast3.getValue().get(str);
						score3 += val.doubleValue();
					}

					if (broadcast4.getValue().containsKey(str)) {
						Double val = broadcast4.getValue().get(str);
						score4 += val.doubleValue();
					}
				}

				if (score1 > 0) {
					pCount++;
				} else if (score2 < 0) {
					nCount++;
				}

				if (score2 > 0) {
					pCount++;
				} else if (score2 < 0) {
					nCount++;
				}

				if (score3 > 0) {
					pCount++;
				} else if (score2 < 0) {
					nCount++;
				}

				if (score4 > 0) {
					pCount++;
				} else if (score2 < 0) {
					nCount++;
				}

				double score = 0.0;

				if (nCount > pCount) {
					score = -1.0;
				} else if (nCount < pCount) {
					score = 1.0;
				}

				tweetContainer.score = score;
				return tweetContainer;
			}
		});

		//tweetResults.coalesce(1).saveAsTextFile("C:/Users/Mayur/workspace/bigdata-project/Output" + Math.random());

		StructType xactSchema = new StructType(
				new StructField[] { new StructField("Tweet", DataTypes.StringType, true, Metadata.empty()),
						new StructField("ratings", DataTypes.DoubleType, true, Metadata.empty()),
						new StructField("place", DataTypes.StringType, true, Metadata.empty()) });

		JavaRDD<Row> rowRDD = tweetResults.map(new Function<TweetContainer, Row>() {

			public Row call(TweetContainer v1) throws Exception {
				Row row = RowFactory.create(v1.tweet, v1.score, v1.placeName);
				return row;
			}
		});

		DataFrame data = sqlContext.createDataFrame(rowRDD, xactSchema).distinct();

		List<String> results = (List<String>) data.toJSON().toJavaRDD().collect();
		saveOutput(results);
	}

	/**
	 * Output file writer
	 * @param output
	 * @throws IOException
	 */
	private static void saveOutput(List<String> output) throws IOException {
		resultsfinalContainer.addAll(output);
		outFP = new BufferedWriter(new FileWriter(outF));
		outFP.write("var array = [");
		for (int i = 0; i < resultsfinalContainer.size(); i++) {
			outFP.write(resultsfinalContainer.get(i));
			if (i != resultsfinalContainer.size() - 1)
				outFP.write(",");
		}
		outFP.write("];");
		outFP.flush();
		outFP.close();
	}

	/**
	 * 
	 * @param tweet
	 * @return
	 */
	private static List<String> tokenizeTweet(String tweet) {
		List<String> tweetWords = new ArrayList<String>();
		for (String word : tweet.toLowerCase().split(" ")) {
			if (isValid(word))
				tweetWords.add(word);
		}
		return tweetWords;
	}

	/**
	 * filter out invalid entries
	 * 
	 * @param word
	 * @return
	 */
	private static boolean isValid(String word) {
		if (word.startsWith("t.") || word.startsWith("http") || word.startsWith("@") || word.startsWith("#"))
			return false;
		return !stopWords.contains(word);
	}
}
