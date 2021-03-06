package com.github.gilvangobbato.entity;

import lombok.*;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

import java.time.LocalDateTime;

@Data
@Builder
@ToString
@DynamoDbBean
@NoArgsConstructor
@AllArgsConstructor
public class Customer {
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String COUNTRY = "country";
    public static final String CITY = "city";
    public static final String PHONE = "phone";
    public static final String STATE = "st";
    public static final String ZIP_CODE = "zip_code";
    public static final String UPDATED_AT = "updated_at";
    public static final String STATE_GSI = "StIndex";

    private String id;
    private String name;
    private String country;
    private String state;
    private String city;
    private String zipCode;
    private String phone;
    private GenderEnum gender;
    private LocalDateTime updatedAt;

    @DynamoDbPartitionKey
    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getCountry() {
        return country;
    }

    @DynamoDbAttribute(Customer.STATE)
    @DynamoDbSecondaryPartitionKey(indexNames = {Customer.STATE_GSI})
    public String getState() {
        return state;
    }

    @DynamoDbAttribute(Customer.CITY)
    @DynamoDbSecondarySortKey(indexNames = {Customer.STATE_GSI})
    public String getCity() {
        return city;
    }

    @DynamoDbAttribute(ZIP_CODE)
    public String getZipCode() {
        return zipCode;
    }

    public String getPhone() {
        return phone;
    }

    public GenderEnum getGender() {
        return gender;
    }

    @DynamoDbAttribute(Customer.UPDATED_AT)
    @DynamoDbConvertedBy(LocalDateTimeConverter.class)
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
}
