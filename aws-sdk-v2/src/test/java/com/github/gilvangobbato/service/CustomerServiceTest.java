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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.utils.Pair;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
                .flatMap(it -> Mono.fromFuture(service.create(it)))
                .block();
    }

    @Test
    void getCustomer() {
        final var customer = this.buildCustomer();
        Mono.just(customer)
                .flatMap(it -> Mono.fromFuture(service.create(it)))
                .then(Mono.defer(() -> Mono.fromFuture(service.getCustomer(customer.getId()))))
                .doOnNext(retrieved -> {
                    log.info(retrieved.getName());
                    Assertions.assertEquals(customer.getId(), retrieved.getId());
                    Assertions.assertEquals(customer.getState(), retrieved.getState());
                    Assertions.assertEquals(customer.getGender(), retrieved.getGender());
                    Assertions.assertEquals(customer.getName(), retrieved.getName());
                    Assertions.assertEquals(customer.getUpdatedAt(), retrieved.getUpdatedAt());
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
                .flatMap(it -> Mono.fromFuture(service.create(it)))
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

    @Test
    void createWithTransaction() {
        List<Customer> customers = new ArrayList<>();

        for (int i = 0; i < 25; i++) {
            customers.add(buildCustomer());
        }

        Mono.just(customers)
                .flatMap(it -> Mono.fromFuture(this.service.createWithTransaction(it)))
                .block();
    }

    @Test
    void createWithBatch() {
        List<Customer> customers = new ArrayList<>();

        for (int i = 0; i < 25; i++) {
            customers.add(buildCustomer());
        }

        Mono.just(customers)
                .flatMap(it -> this.service.createWithBatch(it))
                .block();
    }

    @Test
    void populateTable() {
        final var timeStart = LocalTime.now();
        for (int i = 0; i < 1000; i++) {
            createWithTransaction();
        }
        log.info("Time " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS) + "ms");
    }

    @Test
    void populateWithBatch() {
        final var timeStart = LocalTime.now();
        for (int i = 0; i < 1000; i++) {
            createWithBatch();
        }
        log.info("Time " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS) + "ms");
    }


    @Test
    void populateWithBatchGdi() {
        final var timeStart = LocalTime.now();
        for (int i = 0; i < 40; i++) {
            List<Customer> list = new ArrayList<>();
            for (int j = 0; j < 25; j++) {
                Customer customer = this.buildCustomer();
                customer.setCity("Garibaldi");
                customer.setState("RS");
                customer.setCountry("Brasil");
                list.add(customer);
            }
            this.service.createWithBatch(list).block();
        }
        log.info("Time " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS) + "ms");
    }

    @Test
    void updateTransaction() throws ExecutionException, InterruptedException {
        final var city = "Farroupilha";
        final var zip = "54846001";
        final var state = "RS";
        List<Customer> toUpdate = Arrays.asList(
                this.buildCustomer(),
                this.buildCustomer());
        service.createWithTransaction(toUpdate).get();

        toUpdate.forEach(it -> {
            it.setCity(city);
            it.setZipCode(zip);
            it.setState(state);
        });

        service.updateTransaction(toUpdate).get();

        final var source = Flux.fromIterable(toUpdate)
                .flatMap(it -> Mono.fromFuture(service.getCustomer(it.getId()))
                        .map(retrieved -> Pair.of(it, retrieved)));

        StepVerifier.create(source)
                .consumeNextWith(this::assertCustomer)
                .consumeNextWith(this::assertCustomer)
                .expectComplete()
                .verify();

    }

    @Test
    void queryByStateAndCity() {
        final var timeStart = LocalTime.now();

        List<Customer> customers = Flux.from(service.queryByStateAndCity("RS", "Garibaldi"))
                .doOnNext(page -> log.info("Found " + (page.items().size())))
                .flatMapIterable(Page::items)
//                .doOnNext(it -> log.info("Customer: " + it.getName()))
                .collectList()
                .doOnNext(it -> log.info("Found " + it.size() + " customers in " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS) + "ms"))
                .block();

        assert customers != null;
        log.info("Found " + customers.size() + " customers in " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS) + "ms");
    }

    @Test
    void scanByStateAndCity() {
        final var timeStart = LocalTime.now();

        Flux.from(service.scanByStateAndCity("RS", "Garibaldi"))
                .doOnNext(page -> log.info("Found " + (page.items().size())))
                .flatMapIterable(Page::items)
//                .doOnNext(it -> log.info("Customer: " + it.getName()))
                .collectList()
                .doOnNext(it -> log.info("Found " + it.size() + " customers in " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS) + "ms"))
                .block();
    }

    @Test
    void getBatchItems() {
        List<String> itemsToRead = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            List<Customer> list = new ArrayList<>();
            for (int j = 0; j < 25; j++) {
                Customer customer = this.buildCustomer();
                customer.setCity("Garibaldi");
                customer.setState("RS");
                customer.setCountry("Brasil");
                itemsToRead.add(customer.getId());
                list.add(customer);
            }
            this.service.createWithBatch(list).block();
        }
        Mono.just(itemsToRead)
                .flatMapMany(it -> Flux.from(this.service.batchGetItems(it)))
                .doOnNext(it -> log.info(it.getName()))
                .blockLast();
    }

    private void assertCustomer(Pair<Customer, Customer> pair) {
        final var original = pair.left();
        final var retrieved = pair.right();
        log.info(retrieved.getName());
        Assertions.assertNotNull(retrieved);

        Assertions.assertEquals(original.getCity(), retrieved.getCity());
        Assertions.assertEquals(original.getZipCode(), retrieved.getZipCode());
        Assertions.assertEquals(original.getState(), retrieved.getState());
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
                .updatedAt(LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    @Test
    void scanByState() {
        final var timeStart = LocalTime.now();

        Flux.just("RS", "PA", "ME", "NY", "OR", "MS")
//                .flatMap(it -> service.scanByState(it), 1)
//                .flatMap(it -> service.scanByState(it), 2)
                .flatMap(it -> service.scanByState(it), 5)
//                .flatMap(it -> service.scanByState(it), 10)
//                .flatMap(it -> service.scanByState(it))
                .flatMapIterable(Page::items)
                .collectList()
                .doOnNext(it -> log.info("Found " + it.size() + " customers in " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS) + "ms"))
                .block();

    }

}