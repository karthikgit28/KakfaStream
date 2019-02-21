package com.kafka.stream.kafkastream.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Service;

import com.google.gson.JsonParser;

@Service
public class KafkaStreamClass {

	public void createStream() {

		//Create Proeprty
		Properties property = new Properties();
		property.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		property.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-group");
		property.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		property.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		//Create Topology
		StreamsBuilder streamBuilder = new StreamsBuilder();

		//Input Topic
		KStream<String, String> inputTopic = streamBuilder.stream("twitter_tweets");
		KStream<String, String> filteredStream = inputTopic.filter((k,jsonTweet) -> extractUserFromTweet(jsonTweet) > 10000);
		
		//Move to diff topic
		filteredStream.to("important_tweets");
		
		//Build topology
		KafkaStreams kafkaStream = new KafkaStreams(streamBuilder.build(), property);
		
		//Start the stream
		kafkaStream.start();

	}

	private Integer extractUserFromTweet(String jsonTweet) {
		JsonParser parser = new JsonParser();
		try {
			return parser.parse(jsonTweet)
					.getAsJsonObject()
					.get("user")
					.getAsJsonObject()
					.get("followers_count")
					.getAsInt();
		}catch(Exception e) {
			return 0;
		}

	}


}
