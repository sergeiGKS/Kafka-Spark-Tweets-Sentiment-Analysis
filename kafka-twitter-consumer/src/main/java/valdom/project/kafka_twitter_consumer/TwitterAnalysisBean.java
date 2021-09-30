package valdom.project.kafka_twitter_consumer;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import org.apache.commons.lang3.StringUtils;

import scala.Serializable;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;


public class TwitterAnalysisBean implements Serializable {

    private static Properties properties = new Properties();
    private static StanfordCoreNLP pipeline;

    static {
        properties.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment");
        pipeline = new StanfordCoreNLP(properties);
    }

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
    



	public static TwitterAnalysisBean applySentimentalAnalysis(TwitterAnalysisBean bean) {
    	
        bean.setTweet(StringUtils.remove(bean.getTweet(), "."));
        Annotation annotation = pipeline.process(bean.getTweet());
        List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            bean.setSentiment(sentiment);
        }
    
        bean.setTimeStamp(Calendar.getInstance().getTime());
        
        if (bean.getLat() != null && bean.getLon()!=null) {
            bean.location = bean.getLat()+","+bean.getLon();

        	
        }
        

        
        return bean;
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