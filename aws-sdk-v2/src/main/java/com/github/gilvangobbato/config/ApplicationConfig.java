package com.github.gilvangobbato.config;

import com.github.gilvangobbato.repository.CustomerRepository;
import com.github.gilvangobbato.service.CustomerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.net.URI;
import java.util.concurrent.ExecutionException;

@Configuration
public class ApplicationConfig {

    @Bean
    DynamoDbAsyncClient amazonDynamoDB(@Value("${aws.endpoint}") String endpoint,
                                       @Value("${aws.region}") String region) {
        return DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("123", "123")
                ))
                .region(Region.of(region))
                .build();
    }

    @Bean
    public DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient(final DynamoDbAsyncClient asyncClient) {
        return DynamoDbEnhancedAsyncClient.builder()
                .dynamoDbClient(asyncClient)
                .build();
    }

    @Bean
    public CustomerService service(final CustomerRepository repository) {
        return new CustomerService(repository);
    }

    @Bean
    CustomerRepository repository(final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) throws ExecutionException, InterruptedException {
        return new CustomerRepository(dynamoDbEnhancedAsyncClient);
    }
}
