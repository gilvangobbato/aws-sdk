package com.github.gilvangobbato.entity;

import java.util.Map;
import java.util.Objects;

public class CustomerMapper {

    public static Map<String, Object> toMap(Customer customer) {
        return Map.of(
                Customer.ID, customer.getId(),
                Customer.CITY, customer.getCity(),
                Customer.COUNTRY, customer.getCountry(),
                Customer.NAME, customer.getName(),
                Customer.PHONE, customer.getPhone(),
                Customer.STATE, customer.getState(),
                Customer.ZIP_CODE, customer.getZipCode(),
                Customer.GENDER, customer.getGender().name()
        );
    }

    public static Customer fromMap(Map<String, Object> map) {
        return Customer.builder()
                .id(Objects.toString(map.get(Customer.ID)))
                .city(Objects.toString(map.get(Customer.CITY)))
                .country(Objects.toString(map.get(Customer.COUNTRY)))
                .name(Objects.toString(map.get(Customer.NAME)))
                .phone(Objects.toString(map.get(Customer.PHONE)))
                .state(Objects.toString(map.get(Customer.STATE)))
                .zipCode(Objects.toString(map.get(Customer.ZIP_CODE)))
                .gender(GenderEnum.valueOf(Objects.toString(map.get(Customer.GENDER))))
                .build();
    }
}
