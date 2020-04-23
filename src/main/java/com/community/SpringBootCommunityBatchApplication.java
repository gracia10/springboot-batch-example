package com.community;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBootCommunityBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootCommunityBatchApplication.class, args);
	}
}
