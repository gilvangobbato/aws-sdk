package com.github.gilvangobbato.repository;

import com.github.gilvangobbato.entity.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
public class CustomerRepository {

    final String TABLE_NAME = "Customer";
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;

    private DynamoDbAsyncTable<Customer> getTable() {
        DynamoDbAsyncTable<Customer> table = dynamoDbEnhancedAsyncClient.table(TABLE_NAME, TableSchema.fromBean(Customer.class));
        try {
            table.createTable().get();
        } catch (InterruptedException | ExecutionException e) {
            log.info("Table already exists");
        }
        return table;
    }

    public CompletableFuture<Void> deleteTable() {
        DynamoDbAsyncTable<Customer> dynamoDB = this.getTable();
        return dynamoDB.deleteTable();
    }

    public CompletableFuture<Void> simpleCreate(Customer customer) {
        PutItemEnhancedRequest<Customer> add = PutItemEnhancedRequest
                .builder(Customer.class)
                .conditionExpression(Expression.builder()
                        .expression("attribute_not_exists(id)")
                        .build()
                )
                .item(customer)
                .build();
        return getTable().putItem(add);
    }

    public void createWithTransaction(List<Customer> customers) {

    }

    public CompletableFuture<Customer> getCustomer(String id) {
        return getTable().getItem(Key.builder()
                .partitionValue(id)
                .build());
    }

    public Object queryByStateAndCity(String state, String city) {
        return null;
    }

    public Object scanByStateAndCidy(String state, String city) {
        return null;
    }

    public CompletableFuture<Customer> update(Customer customer) {
        return getTable()
                .updateItem(customer);
    }

    public void updateTransaction(List<Customer> customers) {

    }

}
