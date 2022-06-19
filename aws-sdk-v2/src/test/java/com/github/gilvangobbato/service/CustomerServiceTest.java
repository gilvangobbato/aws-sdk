package com.github.gilvangobbato.service;

import com.github.gilvangobbato.entity.Customer;
import com.github.gilvangobbato.entity.GenderEnum;
import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootTest
@TestMethodOrder(MethodOrderer.MethodName.class)
class CustomerServiceTest {

    @Autowired
    private CustomerService service;
    Lorem lorem = LoremIpsum.getInstance();

    @Test
    void deleteTable() throws ExecutionException, InterruptedException {
        Mono.fromFuture(service.deleteTable())
                .block();
    }

    @Test
    void simpleCreateCustomer() {
        final var customer = this.buildCustomer();
        Mono.just(customer)
                .flatMap(it -> Mono.fromFuture(service.simpleCreateCustomer(it)))
                .block();
    }

    @Test
    void getCustomer() {
        final var customer = this.buildCustomer();
        Mono.just(customer)
                .flatMap(it -> Mono.fromFuture(service.simpleCreateCustomer(it)))
                .then(Mono.defer(() -> Mono.fromFuture(service.getCustomer(customer.getId()))))
                .doOnNext(retrieved -> {
                    log.info(retrieved.getName());
                    Assertions.assertEquals(customer.getId(), retrieved.getId());
                    Assertions.assertEquals(customer.getState(), retrieved.getState());
                    Assertions.assertEquals(customer.getGender(), retrieved.getGender());
                    Assertions.assertEquals(customer.getName(), retrieved.getName());
                })
                .block();

    }

    @Test
    void updateCustomer() {
        final String city = "Garibaldi";
        final String state = "RS";
        final var customer = this.buildCustomer();

        //Create
        Mono.just(customer)
                .flatMap(it -> Mono.fromFuture(service.simpleCreateCustomer(it)))
                .then(Mono.defer(() -> Mono.fromFuture(service.getCustomer(customer.getId()))))
                .doOnNext(it -> {
                    it.setState(state);
                    it.setCity(city);
                })
                .flatMap(it -> Mono.fromFuture(service.update(it)))
                .flatMap(it -> Mono.fromFuture(service.getCustomer(it.getId())))
                .doOnNext(retrieved -> {
                    log.info(retrieved.getName());
                    Assertions.assertEquals(customer.getId(), retrieved.getId());

                    Assertions.assertEquals(state, retrieved.getState());
                    Assertions.assertEquals(city, retrieved.getCity());

                    Assertions.assertEquals(customer.getGender(), retrieved.getGender());
                    Assertions.assertEquals(customer.getName(), retrieved.getName());
                })
                .block();
    }

    private Customer buildCustomer() {
        final var gender = GenderEnum.choseGender();

        return Customer.builder()
                .id(UUID.randomUUID().toString())
                .name(gender.isF() ? lorem.getNameFemale() : lorem.getNameMale())
                .city(lorem.getCity())
                .country(lorem.getCountry())
                .phone(lorem.getPhone())
                .state(lorem.getStateAbbr())
                .zipCode(lorem.getZipCode())
                .gender(gender)
                .build();
    }

}