package com.github.gilvangobbato.repository;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import com.github.gilvangobbato.entity.Customer;
import com.github.gilvangobbato.entity.CustomerMapper;
import com.github.gilvangobbato.entity.CustomerResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class CustomerRepository {

    final String TABLE_NAME = "Customer";
    final DynamoDB dynamoDB;
    final AmazonDynamoDBAsync amazonDynamoDB;

    public void createTable() {
        if (!this.existTable()) {
            try {
                dynamoDB.createTable(new CreateTableRequest()
                        .withTableName(TABLE_NAME)
                        .withAttributeDefinitions(new AttributeDefinition(Customer.ID, ScalarAttributeType.S),
                                new AttributeDefinition(Customer.CITY, ScalarAttributeType.S),
                                new AttributeDefinition(Customer.STATE, ScalarAttributeType.S))
                        .withGlobalSecondaryIndexes(this.buildSecondaryIndex())
                        .withKeySchema(new KeySchemaElement(Customer.ID, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(10L, 5L)));
            } catch (AmazonServiceException ex) {
                System.out.println(ex.getLocalizedMessage());
            }
        }
    }

    private List<GlobalSecondaryIndex> buildSecondaryIndex() {

        ArrayList<KeySchemaElement> stateKey = new ArrayList<>();
        stateKey.add(new KeySchemaElement()
                .withAttributeName(Customer.STATE)
                .withKeyType(KeyType.HASH));  //Partition key
        stateKey.add(new KeySchemaElement()
                .withAttributeName(Customer.CITY)
                .withKeyType(KeyType.RANGE));  //Sort key

        List<GlobalSecondaryIndex> gsi = new ArrayList<>();
        gsi.add(new GlobalSecondaryIndex()
                .withIndexName(Customer.STATE_GSI)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits((long) 10)
                        .withWriteCapacityUnits((long) 10))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withKeySchema(stateKey));

        return gsi;
    }

    public void deleteTable() {
        if (existTable())
            dynamoDB.getTable(TABLE_NAME).delete();
    }

    private boolean existTable() {
        try {
            amazonDynamoDB.describeTable(TABLE_NAME);
            return true;
        } catch (ResourceNotFoundException ex) {
            log.error("Table not exists");
            return false;
        }
    }

    public void simpleCreate(Customer customer) {
        Table table = dynamoDB.getTable(TABLE_NAME);

        table.putItem(this.createItem(customer));

    }

    public void createWithTransaction(List<Customer> customers) {

        List<TransactWriteItem> transaction = new ArrayList<>();

        customers.forEach(customer -> {

            final Map<String, Object> map = CustomerMapper.toMap(customer);
            transaction.add(
                    new TransactWriteItem().withPut(new Put()
                            .withTableName(TABLE_NAME)
                            .withItem(ItemUtils.fromSimpleMap(map))
                            .withConditionExpression("attribute_not_exists(id)")
                    )
            );

        });

        amazonDynamoDB.transactWriteItems(new TransactWriteItemsRequest()
                .withTransactItems(transaction));

    }

    public Customer getCustomer(String id) {
        if (!existTable()) {
            throw new RuntimeException("Table not created");
        }

        Table table = dynamoDB.getTable(TABLE_NAME);

        Item item = table.getItem(new PrimaryKey(Customer.ID, id));

        return CustomerMapper.fromMap(item.asMap());

    }

    public CustomerResponse queryByStateAndCity(String state, String city, Integer limit, Map<String, AttributeValue> lastKey) {
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(TABLE_NAME)
                .withIndexName(Customer.STATE_GSI)
                .withKeyConditionExpression("st = :v_st and city = :v_city")
                .withExpressionAttributeValues(
                        Map.of(":v_st", new AttributeValue(state),
                                ":v_city", new AttributeValue(city)))
                .withExclusiveStartKey(lastKey)
                .withLimit(limit);

        QueryResult result = amazonDynamoDB.query(queryRequest);
        log.info("Scanned " + result.getScannedCount() + " Found " + result.getCount());
        CustomerResponse response = new CustomerResponse();
        if (result.getItems().size() > 0) {
            result.getItems().forEach(it -> {
                response.add(CustomerMapper.fromMap(ItemUtils.toSimpleMapValue(it)));
            });
            response.setLastKey(result.getLastEvaluatedKey());

            return response;
        }
        return response;
    }

    public CustomerResponse scanByStateAndCity(String state, String city, int limit, Map<String, AttributeValue> lastKey) {
        ScanRequest request = new ScanRequest()
                .withTableName(TABLE_NAME)
                .withFilterExpression("st = :v_st AND city = :v_city")
                .withExpressionAttributeValues(
                        Map.of(":v_st", new AttributeValue(state),
                                ":v_city", new AttributeValue(city)))
                .withExclusiveStartKey(lastKey)
                .withLimit(limit);

        ScanResult result = amazonDynamoDB.scan(request);
        log.info("Scanned " + result.getScannedCount() + " Found " + result.getCount());
        CustomerResponse response = new CustomerResponse();
        if (result.getItems().size() > 0) {
            result.getItems().forEach(it -> {
                response.add(CustomerMapper.fromMap(ItemUtils.toSimpleMapValue(it)));
            });
            response.setLastKey(result.getLastEvaluatedKey());

            return response;
        }
        return response;
    }

    private Item createItem(Customer customer) {
        final var pk = new PrimaryKey(Customer.ID, customer.getId());
        return new Item().withPrimaryKey(pk)
                .with(Customer.NAME, customer.getName())
                .with(Customer.COUNTRY, customer.getCountry())
                .with(Customer.STATE, customer.getState())
                .with(Customer.CITY, customer.getCity())
                .with(Customer.ZIP_CODE, customer.getZipCode())
                .with(Customer.PHONE, customer.getPhone())
                .with(Customer.GENDER, customer.getGender().name());
    }

    public void update(Customer customer) {
        Table table = dynamoDB.getTable(TABLE_NAME);

        table.updateItem(this.updateItem(customer));
    }

    private UpdateItemSpec updateItem(Customer customer) {
        final var pk = new PrimaryKey(Customer.ID, customer.getId());
        return new UpdateItemSpec()
                .withAttributeUpdate(List.of(new AttributeUpdate(Customer.CITY).put(customer.getCity()),
                        new AttributeUpdate(Customer.COUNTRY).put(customer.getCountry()),
                        new AttributeUpdate(Customer.NAME).put(customer.getName()),
                        new AttributeUpdate(Customer.STATE).put(customer.getState()),
                        new AttributeUpdate(Customer.PHONE).put(customer.getPhone()),
                        new AttributeUpdate(Customer.ZIP_CODE).put(customer.getZipCode()),
                        new AttributeUpdate(Customer.GENDER).put(customer.getGender().name())))
                .withPrimaryKey(pk);
    }

    public void updateTransaction(List<Customer> customers) {
        List<TransactWriteItem> transaction = new ArrayList<>();

        customers.forEach(customer -> {

            final Map<String, Object> map = CustomerMapper.toMap(customer);
            transaction.add(
                    new TransactWriteItem().withUpdate(new Update()
                            .withUpdateExpression("SET #city = :v_city, #name = :v_name, #country = :v_country, #phone = :v_phone, #st = :v_st, #zip = :v_zip, #gen = :v_gen")
                            .withExpressionAttributeNames(Map.of(
                                    "#city", Customer.CITY,
                                    "#country", Customer.COUNTRY,
                                    "#name", Customer.NAME,
                                    "#phone", Customer.PHONE,
                                    "#st", Customer.STATE,
                                    "#zip", Customer.ZIP_CODE,
                                    "#gen", Customer.GENDER
                            ))
                            .withExpressionAttributeValues(Map.of(
                                    ":v_city", new AttributeValue(customer.getCity()),
                                    ":v_name", new AttributeValue(customer.getName()),
                                    ":v_country", new AttributeValue(customer.getCountry()),
                                    ":v_phone", new AttributeValue(customer.getPhone()),
                                    ":v_st", new AttributeValue(customer.getState()),
                                    ":v_zip", new AttributeValue(customer.getZipCode()),
                                    ":v_gen", new AttributeValue(customer.getGender().name())
                            ))
                            .withKey(Map.of(Customer.ID, new AttributeValue(customer.getId())))
                            .withTableName(TABLE_NAME)
                    )
            );

        });

        amazonDynamoDB.transactWriteItems(new TransactWriteItemsRequest()
                .withTransactItems(transaction));
    }

}
