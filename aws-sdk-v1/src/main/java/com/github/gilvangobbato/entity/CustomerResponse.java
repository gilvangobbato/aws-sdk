package com.github.gilvangobbato.entity;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
public class CustomerResponse {
    private List<Customer> customers = new LinkedList<>();
    private Map<String, AttributeValue> lastKey;

    public void add(Customer customer) {
        this.customers.add(customer);
    }

    public void addAll(List<Customer> customers) {
        if (customers == null) return;
        this.customers.addAll(customers);
    }


}
