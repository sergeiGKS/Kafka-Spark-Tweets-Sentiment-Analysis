package valdom.project.kafka_twitter_producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;


public class TweetsProducer {


    
    public static final Gson gson = new Gson();
    
    public static void main( String[] argss) {
    	
    	final String querrry_hashtag = "vaccin, vacuna, vacina, Impfstoff, vaccine";// , pfizer covid coronavirus%20vaccin";
    	
    	
    	
        



    	
        //System.out.println("Please give hash tags as command line arguments");
  

    /*public static void main(final String[] args) {
        if (args.length < 1) {
            System.out.println("Please give hash tags as command line arguments");
            return;
        }*/
        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        final Producer producer = new KafkaProducer<String, String>(configProperties);
        System.out.println("Starting Kafka Producer ====================================================");
        final String topicName = "tweets-kafka";
        File file = new File("twitter4j.properties");
        Properties prop = new Properties();
        InputStream is = null;
        try {
            if (file.exists()) {
                is = new FileInputStream(file);
                prop.load(is);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ignore) {
                }
            }
        }
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(prop.getProperty("oauth.consumerKey"))
                .setOAuthConsumerSecret(prop.getProperty("oauth.consumerSecret"))
                .setOAuthAccessToken(prop.getProperty("oauth.accessToken"))
                .setOAuthAccessTokenSecret(prop.getProperty("oauth.accessTokenSecret"));
        TwitterStream twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                System.out.print("Sending === > ");
                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                if (status.getGeoLocation() != null) {
                    System.out.println("GeoLocation Found ==== " + status.getGeoLocation());
                }
                TweetsBean bean = new TweetsBean();

                List<String> queryStrings = Arrays.asList(querrry_hashtag.split(","));
                for (String hash : queryStrings) {
                    if (status.getText().toLowerCase().contains(hash.toLowerCase())) {
                        bean.setTag(hash);
                        
                    }
                }
                
                User user = status.getUser();
                String profileLocation = user.getLocation();
                System.out.println("-------profileLocation ---- " + profileLocation);
                
                if (profileLocation!=null) {
                	
                	ArrayList<String>  loc = bean.getGeolocation(profileLocation);
                	
                	if(loc.size()==4) {
                		
                    	bean.setCountryCode(loc.get(0));
                    	bean.setCountryName(loc.get(1));
                    	bean.setLat(Double.parseDouble(loc.get(2)));
                    	bean.setLon(Double.parseDouble(loc.get(3)));
                		
                	}
                	

                                    	
                }
                
               /* HashtagEntity[] entities = status.getHashtagEntities();
                
                for (HashtagEntity entity : entities) {
                	
                	String htag = entity.getText();
                }*/
                

                
                bean.setTweet(status.getText());
                bean.setId(status.getId());
                bean.setLanguage(status.getLang());
                bean.setCreated(status.getCreatedAt());
                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, gson.toJson(bean));
                producer.send(rec);
                System.out.println("-------producer recording ---- " + rec);

            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning warning) {
                //System.out.println("Got stall warning:" + warning);
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        twitterStream.sample(); //Starts listening on random sample of all public statuses.
        FilterQuery tweetQueryFilter = new FilterQuery();
        tweetQueryFilter.track(querrry_hashtag.split(","));
        twitterStream.filter(tweetQueryFilter);
    }
}
