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

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@SpringBootTest
@TestMethodOrder(MethodOrderer.MethodName.class)
class CustomerServiceTest {

    @Autowired
    private CustomerService service;
    Lorem lorem = LoremIpsum.getInstance();

    @Test
    void deleteTable() {
        service.deleteTable();
    }

    @Test
    void simpleCreateCustomer() {
        service.createTable();
        service.simpleCreateCustomer(this.buildCustomer());
    }

    @Test
    void createWithTransaction() {
        service.createTable();
        List<Customer> customers = new ArrayList<>();

        for (int i = 0; i < 25; i++) {
            customers.add(buildCustomer());
        }

        service.createWithTransaction(customers);
    }

    @Test
    void getCustomer() {
        service.createTable();

        Customer customer = this.buildCustomer();
        service.simpleCreateCustomer(customer);

        Customer fromDynamo = service.getCustomer(customer.getId());

        Assertions.assertNotNull(fromDynamo);
        Assertions.assertEquals(customer.getId(), fromDynamo.getId());
        Assertions.assertEquals(customer.getName(), fromDynamo.getName());
        Assertions.assertEquals(customer.getGender(), fromDynamo.getGender());
    }

    @Test
    void update() {
        service.createTable();

        Customer customer = this.buildCustomer();
        service.simpleCreateCustomer(customer);

        customer.setCity("Farroupilha");
        customer.setZipCode("95885440");
        customer.setState("RS");

        service.update(customer);

        Customer fromDynamo = service.getCustomer(customer.getId());

        Assertions.assertNotNull(fromDynamo);
        Assertions.assertEquals("Farroupilha", fromDynamo.getCity());
        Assertions.assertEquals("95885440", fromDynamo.getZipCode());
        Assertions.assertEquals("RS", fromDynamo.getState());
    }

    @Test
    void updateTransaction() {
        service.createTable();
        final var city = "Farroupilha";
        final var zip = "54846001";
        final var state = "RS";
        List<Customer> toUpdate = List.of(
                this.buildCustomer(),
                this.buildCustomer());
        service.createWithTransaction(toUpdate);

        toUpdate.forEach(it -> {
            it.setCity(city);
            it.setZipCode(zip);
            it.setState(state);
        });

        service.updateTransaction(toUpdate);

        toUpdate.forEach(original -> {
            Customer fromDynamo = service.getCustomer(original.getId());

            Assertions.assertNotNull(fromDynamo);
            Assertions.assertEquals(original.getCity(), fromDynamo.getCity());
            Assertions.assertEquals(original.getZipCode(), fromDynamo.getZipCode());
            Assertions.assertEquals(original.getState(), fromDynamo.getState());
        });
    }

    @Test
    void populateTable() {
        for (int i = 0; i < 1000; i++) {
            createWithTransaction();
            Customer customer = this.buildCustomer();
            customer.setCity("Garibaldi");
            customer.setState("RS");
            service.simpleCreateCustomer(customer);
        }
    }

    @Test
    void queryByStateAndCity() {
        final var timeStart = LocalTime.now();
        List<Customer> customers = service.queryByStateAndCity("RS", "Garibaldi");
        log.info("Found " + customers.size() + " customers in " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS));
    }

    @Test
    void scanByStateAndCity() {
        final var timeStart = LocalTime.now();
        List<Customer> customers = service.scanByStateAndCity("RS", "Garibaldi");
        log.info("Found " + customers.size() + " customers in " + timeStart.until(LocalTime.now(), ChronoUnit.MILLIS));
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