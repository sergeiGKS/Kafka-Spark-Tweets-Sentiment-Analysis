package valdom.project.kafka_twitter_consumer;

import java.util.HashMap;
import java.util.Map;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.apache.spark.streaming.Duration;


import com.google.gson.Gson;

import scala.Tuple2;





public class TweetsConsumer {


    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
    	
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        System.out.println("Starting Kafka Consumer ===========================================================");
        TweetsConsumer consumer = new TweetsConsumer();
        consumer.consume();
        
        
        
    }

    private void consume() {
    
        SparkConf sparkConf = new SparkConf().
        		setMaster("local[4]").setAppName("TweetsConsumer");
        
        //SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        
    	
        //ElasticSearch Conf
        sparkConf.set("es.index.auto.create", "true");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(10000));

        int numOfThreads = 4;
        Map<String, Integer> topicMap1 = new HashMap<String, Integer>();
        topicMap1.put("tweets-kafka", numOfThreads);

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(streamingContext, "localhost", "group-1", topicMap1);

        
        JavaDStream<String> lines = messages.map(Tuple2::_2);
        
        //JavaDStream<TweetsAnalysisML> beans = lines.map(x -> TweetsAnalysisML.sentimentalAnalysis(gson.fromJson(x, TweetsAnalysisML.class),  sparkSession));
      

        
        JavaDStream<TwitterAnalysisBean> beans = lines.map(x -> TwitterAnalysisBean.applySentimentalAnalysis(
                gson.fromJson(x, TwitterAnalysisBean.class)));
               
        //Upload to elasticsearch code
        JavaEsSparkStreaming.saveToEs(beans, "tweetsml/_doc");
        beans.print();
        

        streamingContext.start();
        try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
      

       
    	
    	
        /*//build the spark sesssion
        SparkSession spark = SparkSession.builder().appName("flightdelay").master("local[*]").getOrCreate();

        //set the log level only to log errors
        spark.sparkContext().setLogLevel("ERROR");

        

        //define schema type of file data source
        StructType schema = new StructType().add("id", DataTypes.StringType)
                .add("user_location", DataTypes.StringType).add("text", DataTypes.StringType)
                .add("sentiment_vader", DataTypes.IntegerType);
        


        //build the streaming data reader from the file source, specifying csv file format  
        
        spark.sparkContext().addFile("vaccination_tweets.csv");
        Dataset<Row> rawData = spark.read().option("header", true).format("csv").schema(schema)

                .csv("vaccination_tweets*.csv");

        rawData.createOrReplaceTempView("tweetData");

      //count of employees grouping by department

        Dataset<Row> result = spark.sql("select text,sentiment_vader from  tweetData");
                
        

        
        TweetsAnalysisML analysisML = new TweetsAnalysisML();
        
        try {
        	            
            

			Dataset<Row> tuple2 = analysisML.makePrediction(result);
			
			tuple2.show();
            

            
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        //write stream to output console with update mode as data is being aggregated */

        
        

    }
}