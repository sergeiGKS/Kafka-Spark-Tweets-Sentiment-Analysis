package valdom.project.kafka_twitter_consumer;

import scala.Serializable;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;

import java.util.Arrays;
import java.util.Date;
import java.util.List;


import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


public class TweetsAnalysisML implements Serializable {
	
	private long id;
    private String tag;
    private String sentiment;
    private String tweet;
    private Date timeStamp;
    private Double lat;
    private Double lon;
    private String location;
    private String language;
    private Date created;
    private String countryCode;
    private String countryName;
	
    public static TweetsAnalysisML  sentimentalAnalysis(TweetsAnalysisML bean, SparkSession sparkConf) {
    	
    	
    	Dataset<Row> text_df = stringtoDataset(bean.getTweet(), sparkConf);
    	
        Dataset<Row> sentiments = makePrediction(text_df);
        
        List<String> sentiment= sentiments.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
        System.out.println("----------"+sentiment.get(0));
        
        //sparkConf.clone();
        
        bean.setSentiment(sentiment.get(0));
                
        if (bean.getLat() != null && bean.getLon()!=null) {
            bean.location = bean.getLat()+","+bean.getLon();
        	
        }
        return bean;
    }
    
    
    private static Dataset<Row> stringtoDataset(String text, SparkSession spark) {
    	
    	
    //	SparkSession  spark1 = spark.newSession();

        //SparkSession spark = new SparkSession(sparkContext);
    	
    	Dataset<Row> text_df = spark.createDataset(Arrays.asList(text), Encoders.STRING()).toDF("text");
        //System.out.println("-----------------");
        //text_df.show();
    	
    	//spark1.close();
    	
    	return text_df;
    }	
	
    
	
	
	
	public double trainModel(Dataset<Row> dataset) throws Exception  {
        
    	final int numClusters = 3 ;
        
        Dataset<Row> filteredData = dataset.na().drop();


        Tokenizer tokenizer = new Tokenizer()
        		.setInputCol("text")
        		.setOutputCol("tokens");
        		
        StopWordsRemover stopWordsRemover = new StopWordsRemover()
        		.setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
        		.setInputCol(tokenizer.getOutputCol())
        		.setOutputCol("stopwords");
        
        HashingTF hashingTF = new HashingTF()
        .setInputCol(stopWordsRemover.getOutputCol())
        .setOutputCol("features");
        
        KMeans kmModel = new KMeans();
        
        kmModel.setK(numClusters);
        


        Pipeline pipeline = new Pipeline()
        		.setStages(new PipelineStage[] {tokenizer, stopWordsRemover, hashingTF, kmModel});

        PipelineModel newModel = pipeline.fit(filteredData);
        
       newModel.write().overwrite().save("kMeansModel");

        // Evaluate clustering by computing Silhouette score
        Dataset<Row> predictions = newModel.transform(filteredData);
        ClusteringEvaluator evaluator =  new ClusteringEvaluator();
        
        
        //Dataset<Row> sentiment = predictions.selectExpr("CASE WHEN prediction == 2 THEN  'positive' WHEN prediction == 1 THEN  'negative' ELSE 'neutral' END AS sentiment");

        
        //Dataset<Row>  predictionsTab = predictions.stat().crosstab("sentiment_vader","prediction");

        


        double silhouette = evaluator.evaluate(predictions);
        
        
        
        

        return  silhouette;
    }
    
    
    
    public Tuple2<PipelineModel, Double> ReTrainModel(Dataset<Row> dataset, PipelineModel latestModel ) throws Exception  {

    	
        
        Dataset<Row> filteredData = dataset.na().drop();

        Tokenizer tokenizer = new Tokenizer()
        		.setInputCol("text")
        		.setOutputCol("tokens");
        		
        StopWordsRemover stopWordsRemover = new StopWordsRemover()
        		.setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
        		.setInputCol(tokenizer.getOutputCol())
        		.setOutputCol("stopwords");
        
        HashingTF hashingTF = new HashingTF()
        .setNumFeatures(1000)
        .setInputCol(stopWordsRemover.getOutputCol())
        .setOutputCol("features");
        
   


        Pipeline pipeline = new Pipeline()
        		.setStages(new PipelineStage[] {tokenizer, stopWordsRemover, hashingTF, latestModel});

        PipelineModel newModel = pipeline.fit(filteredData);
        
        newModel.write().overwrite().save("kMeansModel");

        // Evaluate clustering by computing Silhouette score
        Dataset<Row> predictions = newModel.transform(filteredData);
        ClusteringEvaluator evaluator =  new ClusteringEvaluator();


        double silhouette = evaluator.evaluate(predictions);
        

        return new Tuple2<PipelineModel, Double>(newModel, silhouette);
    }
    

    
    
    
    public static Dataset<Row> makePrediction(Dataset<Row> scrappedDataBatch) {
    	
        Dataset<Row> filteredData = scrappedDataBatch.na().drop();

    	
        PipelineModel model = PipelineModel.load("kMeansModel");

        Dataset<Row> sentimentsData = model.transform(filteredData);
        
        Dataset<Row> sentiment = sentimentsData.selectExpr("CASE WHEN prediction == 2 THEN  'positive' WHEN prediction == 1 THEN  'negative' ELSE 'neutral' END AS sentiment");

        return sentiment;
    }


	
    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getSentiment() {
        return sentiment;
    }

    public void setSentiment(String sentiment) {
        this.sentiment = sentiment;
    }

    public String getTweet() {
        return tweet;
    }

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
    
	
	
    public String getCountryName() {
		return countryName;
	}

	public void setCountryName(String countryName) {
		this.countryName = countryName;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

    @Override
    public String toString() {
        return "TwitterAnalysisBean{" +
                "tag='" + tag + '\'' +
                ", sentiment='" + sentiment + '\'' +
                ", tweet='" + tweet + '\'' +
                ", timeStamp=" + timeStamp +
                ", lat=" + lat +
                ", lon=" + lon +
                ", location=" + location +
                ", countryCode="+ countryCode +
                ", countryName="+ countryName +
                ", language="+ language +
                '}';
    }

}
