package com.github.gilvangobbato.service;

import com.github.gilvangobbato.entity.Customer;
import com.github.gilvangobbato.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetResultPagePublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PagePublisher;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@AllArgsConstructor
public class CustomerService {

    final CustomerRepository repository;

    public CompletableFuture<Void> create(Customer customer) {
        return repository.create(customer);
    }

    public CompletableFuture<Void> createWithTransaction(List<Customer> customers) {
        return repository.createWithTransaction(customers);
    }

    public Mono<Boolean> createWithBatch(List<Customer> customers){
        return repository.createWithBatch(customers);
    }

    public CompletableFuture<Void> deleteTable() throws ExecutionException, InterruptedException {
        return repository.deleteTable();
    }

    public CompletableFuture<Customer> getCustomer(String id) {
        return repository.getCustomer(id);
    }


    public CompletableFuture<Customer> update(Customer customer) {
        return repository.update(customer);
    }

    public CompletableFuture<Void> updateTransaction(List<Customer> customers) {
        return repository.updateTransaction(customers);
    }

    public Publisher<Page<Customer>> queryByStateAndCity(String state, String city) {

        return repository.queryByStateAndCity(state, city);
    }

    public PagePublisher<Customer> scanByStateAndCity(String state, String city) {
        return repository.scanByStateAndCity(state, city);
    }

    public SdkPublisher<Customer> batchGetItems(List<String> keys) {
        return repository.batchGetItems(keys);
    }

    public PagePublisher<?> scanByState(String it) {
        return repository.scanByState(it);
    }
}
