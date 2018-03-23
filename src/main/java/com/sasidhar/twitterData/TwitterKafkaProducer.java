
package com.sasidhar.twitterData;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.actors.threadpool.Arrays;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class TwitterKafkaProducer {

	public static void run() throws InterruptedException, IOException {
		FileInputStream input = new FileInputStream("config.properties");
		Properties prop= new Properties();
		prop.load(input);
		Properties properties = new Properties();		
		properties.put("metadata.broker.list", prop.getProperty("brokerList"));
		String topic= prop.getProperty("topic");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id","TweetGenerator");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
		List<String> trackTerms=Arrays.asList((prop.getProperty("trackTerms")).toString().split(","));
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(trackTerms);

		Authentication auth = new OAuth1(prop.getProperty("consumerKey"), prop.getProperty("consumerSecret"), prop.getProperty("accessTocken"),
				prop.getProperty("accessTockenSecret"));
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		// Do whatever needs to be done with messages
		while(true){
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(topic, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}

	}
	public static void main(String[] args) throws IOException {
		try {
			TwitterKafkaProducer.run();
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
