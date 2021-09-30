package valdom.project.kafka_twitter_producer;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Geocoder {

    private static final String GEOCODING_RESOURCE = "https://geocode.search.hereapi.com/v1/geocode";
    private static final String API_KEY = "TdVk1X74Pn8ipZrXNxWwODiv7PtI5HjKWLNjaLmAvok";
    
    //String requestUri = GEOCODING_RESOURCE + "?apiKey=" + API_KEY + "&query=" + encodedQuery;


    public String geocodeSync(String query) throws IOException, InterruptedException {

    	CloseableHttpClient httpClient = HttpClients.createDefault();
    	
        String encodedQuery = URLEncoder.encode(query,"UTF-8");
        String requestUri = GEOCODING_RESOURCE + "?apiKey=" + API_KEY + "&q=" + encodedQuery;
        
        HttpGet geocodingRequest = new HttpGet(requestUri);
        
        try (CloseableHttpResponse response = httpClient.execute(geocodingRequest)) {

            // Get HttpResponse Status
            System.out.println(response.getStatusLine().toString());

            HttpEntity entity = response.getEntity();
            Header headers = entity.getContentType();
            System.out.println(headers);

            // return it as a String
			String result = EntityUtils.toString(entity);
			System.out.println(result);
			
			return result;
        }
        

    }
    
    
    public ArrayList<String> getJsonRsponse( String query) {
    	
    	
    	ObjectMapper mapper = new ObjectMapper();
        Geocoder geocoder = new Geocoder();

        String response;
		try {
			response = geocoder.geocodeSync(query);
	        JsonNode responseJsonNode = mapper.readTree(response);
	        JsonNode items = responseJsonNode.get("items");
	        
	        ArrayList<String> list = new ArrayList<String>();
	        
	        for (JsonNode item : items) {
	            JsonNode address = item.get("address");
	            String countryCode = address.get("countryCode").asText();
	            String countryName = address.get("countryName").asText();

	            JsonNode position = item.get("position");

	            String lat = position.get("lat").asText();
	            String lng = position.get("lng").asText();
	            
	            list.add(countryCode);
	            list.add(countryName);
	            list.add(lat);
	            list.add(lng);
	            

	            
	        }
	        
	        return list;


		} catch (IOException | InterruptedException e) {
			
			return null;
		}



    }
    
    

}