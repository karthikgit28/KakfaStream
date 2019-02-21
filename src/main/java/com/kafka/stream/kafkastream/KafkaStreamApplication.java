package com.kafka.stream.kafkastream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.kafka.stream.kafkastream.streams.KafkaStreamClass;

@SpringBootApplication
public class KafkaStreamApplication implements CommandLineRunner{

	@Autowired
	private KafkaStreamClass streamss;
	
	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		streamss.createStream();
	}

}
