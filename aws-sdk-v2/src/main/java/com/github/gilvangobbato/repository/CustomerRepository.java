package com.github.gilvangobbato.repository;

import com.github.gilvangobbato.entity.Customer;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class CustomerRepository {

    final String TABLE_NAME = "Customer";
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    private final DynamoDbAsyncTable<Customer> table;

    public CustomerRepository(final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
                this.table = dynamoDbEnhancedAsyncClient.table(TABLE_NAME, TableSchema.fromBean(Customer.class));
        try {
            table.createTable().get();
        } catch (InterruptedException | ExecutionException e) {
            log.info("Table already exists");
        }
    }


    public CompletableFuture<Void> deleteTable() {
        return this.table.deleteTable();
    }

    public CompletableFuture<Void> create(Customer customer) {
        PutItemEnhancedRequest<Customer> add = PutItemEnhancedRequest
                .builder(Customer.class)
                .conditionExpression(Expression.builder()
                        .expression("attribute_not_exists(id)")
                        .build()
                )
                .item(customer)
                .build();
        return this.table.putItem(add);
    }

    public CompletableFuture<Void> createWithTransaction(List<Customer> customers) {
        final var request = TransactWriteItemsEnhancedRequest.builder();

        customers.forEach(customer -> {
            request.addPutItem(this.table, customer);
        });

        return dynamoDbEnhancedAsyncClient.transactWriteItems(request.build());
    }

    public CompletableFuture<Customer> getCustomer(String id) {
        return this.table.getItem(Key.builder()
                .partitionValue(id)
                .build());
    }

    public CompletableFuture<Customer> update(Customer customer) {
        return this.table.updateItem(customer);
    }

    public CompletableFuture<Void> updateTransaction(List<Customer> customers) {
        final var request = TransactWriteItemsEnhancedRequest.builder();

        customers.forEach(customer -> {
            request.addUpdateItem(this.table, customer);
        });

        return dynamoDbEnhancedAsyncClient.transactWriteItems(request.build());
    }

    public SdkPublisher<Page<Customer>> queryByStateAndCity(String state, String city) {
        DynamoDbAsyncIndex<Customer> secIndex = dynamoDbEnhancedAsyncClient
                .table(TABLE_NAME, TableSchema.fromBean(Customer.class))
                .index(Customer.STATE_GSI);

        AttributeValue pk = AttributeValue.builder()
                .s(state)
                .build();
        AttributeValue sk = AttributeValue.builder()
                .s(city)
                .build();

        QueryConditional query = QueryConditional.keyEqualTo(Key.builder()
                .partitionValue(pk)
                .sortValue(sk)
                .build());

        return secIndex.query(QueryEnhancedRequest.builder()
                .queryConditional(query)
                .limit(10)
                .build());
    }

    public Object scanByStateAndCity(String state, String city) {
        return null;
    }

}
