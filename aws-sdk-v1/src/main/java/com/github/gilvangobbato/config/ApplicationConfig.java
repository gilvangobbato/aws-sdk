package com.github.gilvangobbato.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.github.gilvangobbato.repository.CustomerRepository;
import com.github.gilvangobbato.service.CustomerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfig {

    @Bean
    AmazonDynamoDBAsync amazonDynamoDB(@Value("${aws.endpoint}") String endpoint,
                                       @Value("${aws.region}") String region) {
        return AmazonDynamoDBAsyncClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials("123", "123"))
                )
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .build();
    }

    @Bean
    DynamoDB dynamoDB(AmazonDynamoDBAsync amazonDynamoDB) {
        return new DynamoDB(amazonDynamoDB);
    }

    @Bean
    CustomerRepository repository(DynamoDB dynamoDB, AmazonDynamoDBAsync amazonDynamoDB) {
        return new CustomerRepository(dynamoDB, amazonDynamoDB);
    }

    @Bean
    CustomerService clientService(CustomerRepository repository) {
        return new CustomerService(repository);
    }
}
