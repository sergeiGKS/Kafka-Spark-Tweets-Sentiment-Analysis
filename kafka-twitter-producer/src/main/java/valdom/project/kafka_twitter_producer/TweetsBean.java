package valdom.project.kafka_twitter_producer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;


public class TweetsBean implements Serializable {
	

	private static final long serialVersionUID = 1L;
	
	
	private long id;
	private String tag;
    private String sentiment;
    private String tweet;
    private Date timeStamp;
    private Double lat;
    private Double lon;
    private String language;
    private Date created;
    private String countryCode;
    private String countryName;
    
    public String getCountryName() {
		return countryName;
	}

	public void setCountryName(String countryName) {
		this.countryName = countryName;
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


	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}


	
	
	
	public ArrayList<String> getGeolocation(String profilLocation) {
		
		String[] arrayList = profilLocation.split(",");
       	 	
        String s = arrayList[arrayList.length-1];
        
        Geocoder geocoder = new Geocoder();
        
        ArrayList<String> list = geocoder.getJsonRsponse(s);
        	
        return list;
			
        }

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
        
       
		

		


}