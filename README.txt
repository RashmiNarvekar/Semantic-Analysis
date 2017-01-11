How to execute?

Miner folder contains the entire eclipse project.

1)We have used 4 dictionaries and they are included in the zip file. For using these dictionaries, paths for these dictionaries in the DictionaryBuilder.java class has to be changed based on the current location in filesystem.

2)Additionally location for output file has to be changed in the code.
This output file contains tweet with there respective sentiments.(1 -> positive 0 -> Neutral -1 -> Negative)

3) Update the location of Stopwords.csv in DictionaryBuilder.java class.

4) For retrieving tweets, one has to provide twitter keys.

eg:
        System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY);
		System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET);
		System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN);
		System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET);
		
Update these keys in DictionaryBuilder.java class.

5) Once all the above changes have been made, for viewing piechart, output files location must be updated in PieChartViewer.html file.


Library and Frameworks:
Apache Spark
twitter4j
Highcharts.js
jQuery		
