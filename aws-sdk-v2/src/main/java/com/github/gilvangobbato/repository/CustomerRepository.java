package com.github.gilvangobbato.repository;

import com.github.gilvangobbato.entity.Customer;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class CustomerRepository {

    final String TABLE_NAME = "Customer";
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    private final DynamoDbAsyncTable<Customer> table;

    public CustomerRepository(final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
        this.table = dynamoDbEnhancedAsyncClient.table(TABLE_NAME, TableSchema.fromBean(Customer.class));
        try {
            table.createTable(CreateTableEnhancedRequest.builder()
                    .globalSecondaryIndices(EnhancedGlobalSecondaryIndex.builder()
                            .indexName(Customer.STATE_GSI)
                            .projection(Projection.builder()
                                    .projectionType(ProjectionType.ALL)
                                    .build())
                            .provisionedThroughput(ProvisionedThroughput.builder()
                                    .readCapacityUnits(10L)
                                    .writeCapacityUnits(10L)
                                    .build())
                            .build())
                    .build()).get();
        } catch (InterruptedException | ExecutionException e) {
            log.info("Table already exists");
        }
    }


    public CompletableFuture<Void> deleteTable() {
        return dynamoDbEnhancedAsyncClient.table(TABLE_NAME, TableSchema.fromBean(Customer.class)).deleteTable();
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

    public Mono<Boolean> createWithBatch(List<Customer> customers) {
        final var request = BatchWriteItemEnhancedRequest.builder();

        customers.forEach(customer -> {
            request.addWriteBatch(WriteBatch.builder(Customer.class)
                    .addPutItem(customer)
                    .mappedTableResource(this.table)
                    .build());
        });

        return Mono.fromFuture(dynamoDbEnhancedAsyncClient.batchWriteItem(request.build()))
                .thenReturn(Boolean.TRUE);
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

    public SdkPublisher<Customer> batchGetItems(List<String> keys) {
        return dynamoDbEnhancedAsyncClient.batchGetItem(BatchGetItemEnhancedRequest.builder()
                        .readBatches(keys.stream().map(it -> ReadBatch.builder(Customer.class)
                                .addGetItem(Key.builder().partitionValue(it).build())
                                .mappedTableResource(table)
                                .build()).collect(Collectors.toList()))
                        .build())
                .resultsForTable(this.table);
    }

    public Publisher<Page<Customer>> queryByStateAndCity(String state, String city) {
        DynamoDbAsyncIndex<Customer> secIndex = this.table.index(Customer.STATE_GSI);

        QueryConditional query = QueryConditional.keyEqualTo(Key.builder()
                .partitionValue(state)
                .sortValue(city)
                .build());

        return secIndex.query(QueryEnhancedRequest.builder()
                .queryConditional(query)
                .limit(100)
                .build());
    }

    public PagePublisher<Customer> scanByStateAndCity(String state, String city) {
        ScanEnhancedRequest request = ScanEnhancedRequest.builder()
                .filterExpression(Expression.builder()
                        .expression("#st = :v_st AND #city = :v_city")
                        .expressionNames(Map.of("#st", Customer.STATE, "#city", Customer.CITY))
                        .expressionValues(Map.of(":v_st", AttributeValue.fromS(state),
                                ":v_city", AttributeValue.fromS(city)))
                        .build())
                .limit(100)
                .build();
        return this.table.scan(request);
    }

}
