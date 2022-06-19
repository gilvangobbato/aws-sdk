package com.github.gilvangobbato.service;

import com.github.gilvangobbato.entity.Customer;
import com.github.gilvangobbato.entity.CustomerResponse;
import com.github.gilvangobbato.repository.CustomerRepository;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class CustomerService {

    final CustomerRepository repository;

    public void simpleCreateCustomer(Customer customer) {
        repository.simpleCreate(customer);
    }

    public void createWithTransaction(List<Customer> customers) {
        repository.createWithTransaction(customers);
    }

    public void createTable() {
        repository.createTable();
    }

    public void deleteTable() {
        repository.deleteTable();
    }

    public Customer getCustomer(String id) {
        return repository.getCustomer(id);
    }

    public List<Customer> queryByStateAndCity(String state, String city) {
        CustomerResponse response = new CustomerResponse();
        do {
            CustomerResponse fromQuery = repository.queryByStateAndCity(state, city, 100, response.getLastKey());
            response.addAll(fromQuery.getCustomers());
            response.setLastKey(fromQuery.getLastKey());
        } while (response.getLastKey() != null);

        return response.getCustomers();
    }

    public List<Customer> scanByStateAndCity(String state, String city) {
        CustomerResponse response = new CustomerResponse();
        do {
            CustomerResponse fromQuery = repository.scanByStateAndCidy(state, city, 1000, response.getLastKey());
            response.addAll(fromQuery.getCustomers());
            response.setLastKey(fromQuery.getLastKey());
        } while (response.getLastKey() != null);
        return response.getCustomers();
    }

    public void update(Customer customer) {
        repository.update(customer);
    }

    public void updateTransaction(List<Customer> customers) {
        repository.updateTransaction(customers);
    }
}
