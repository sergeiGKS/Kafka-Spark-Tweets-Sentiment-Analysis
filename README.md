# Description:

This project contains a Scrapper component (Producer) which a Java program that uses the Twitter4J to stream real time tweets that have the given hash tags.
The scrapper program writes the data to a Kafka server with Zookeeper.

The second component (consumer) is a Java program that reads data using Spark streaming and then uses Stanford CoreNLP to determine the sentiment of the tweet. This program then writes the data to an elasticsearch document which can be visualized by using Kibana.


# Steps to run the project:

## 1. Install 

	Apache Spark : Spark-2.2.0-bin-hadoop2.7
	Apache Kafka : kafka_2.11-2.1.0
	nginx 1.18.0
	Elasticsearch 7.10.2
	Kibana 7.10
	Apache Maven 3.6.3


## 2. Import  kafka-twitter-producer and kafka-twitter-consumer projects as Java Maven projects

## 3. Start Kafka server :
	$ _sudo systemctl start kafka_
	
## 4. Start start elasticsearch and kibana servers :
	$ _sudo systemctl start elasticsearch_
	$ _sudo systemctl start kibana_
	
## 5. In consumer directory, build jar dependency file :

	$ _mvn clean compile assembly:single_


## 5. Run the Producer program as simple java project

## 6. Run the Consumer program : 

	$ _spark-submit --class valdom.project.kafka_twitter_consumer.TweetsConsumer --master local[4] target/kafka-twitter-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar_
	
## 7. open Kibana UI (localhost:5601) and visualize your data. 
