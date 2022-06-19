package com.github.gilvangobbato.service;

import com.github.gilvangobbato.entity.Customer;
import com.github.gilvangobbato.repository.CustomerRepository;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@AllArgsConstructor
public class CustomerService {

    final CustomerRepository repository;

    public CompletableFuture<Void> simpleCreateCustomer(Customer customer) {
        return repository.simpleCreate(customer);
    }

    public void createWithTransaction(List<Customer> customers) {
        repository.createWithTransaction(customers);
    }

    public CompletableFuture<Void> deleteTable() throws ExecutionException, InterruptedException {
        return repository.deleteTable();
    }

    public CompletableFuture<Customer> getCustomer(String id) {
        return repository.getCustomer(id);
    }

    public List<Customer> queryByStateAndCity(String state, String city) {

        return null;
    }

    public List<Customer> scanByStateAndCity(String state, String city) {
        return null;
    }

    public CompletableFuture<Customer> update(Customer customer) {
        return repository.update(customer);
    }

    public void updateTransaction(List<Customer> customers) {
        repository.updateTransaction(customers);
    }
}
