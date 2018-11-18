package com.bszeliga.ignite.server;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@SpringBootApplication
public class IgniteServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(IgniteServerApplication.class, args);
    }
}


@Configuration
class IgniteServerConfiguration {

    @Bean
    public Ignite ignite() {
        return Ignition.start();
    }

    @Bean
    IgniteCache<Long, Person> personCache(@Autowired Ignite ignite) {
        return ignite.getOrCreateCache("PERSON_CACHE");
    }

    @Bean
    IgniteAtomicLong personIdGenerator(@Autowired Ignite ignite) {
        return ignite.atomicLong("PERSON_ID_GEN", 0, true);
    }
}

@RestController
class PersonController {

    @Autowired
    IgniteAtomicLong personIdGenerator;

    @Autowired
    IgniteCache<Long, Person> personCache;

    @GetMapping(value = "/persons")
    ResponseEntity<List<PersonResponse>> getPersons() {
        List<PersonResponse> responeList = StreamSupport.stream(personCache.spliterator(), false)
                .map(it -> new PersonResponse(it.getKey(), it.getValue()))
                .collect(Collectors.toList());

        return ResponseEntity.ok(responeList);
    }

    @GetMapping(value = "/persons/{id}")
    ResponseEntity<Person> getPerson(@PathVariable(name = "id") long id) {
        return ResponseEntity.of(Optional.ofNullable(personCache.get(id)));
    }

    @PostMapping(value = "/persons/{name}")
    ResponseEntity<PersonResponse> createPerson(@PathVariable(name = "name") String name) {
        Person p = new Person(name);
        long id = personIdGenerator.getAndIncrement();

        personCache.putIfAbsent(id, p);

        return ResponseEntity.ok(new PersonResponse(id, p));
    }

}

class PersonResponse {
    long id;
    Person person;

    public PersonResponse(Long id, Person person) {
        this.id = id;
        this.person = person;
    }

    public long getId() {
        return id;
    }

    public Person getPerson() {
        return person;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonResponse that = (PersonResponse) o;
        return id == that.id &&
                Objects.equals(person, that.person);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, person);
    }

    @Override
    public String toString() {
        return "PersonResponse{" +
                "id=" + id +
                ", person=" + person +
                '}';
    }
}

class Person {
    private String name;

    public Person(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(name, person.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                '}';
    }
}