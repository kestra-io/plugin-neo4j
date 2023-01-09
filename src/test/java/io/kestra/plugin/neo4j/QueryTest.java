package io.kestra.plugin.neo4j;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test will only test the main task, this allow you to send any input
 * parameters to your task and test the returning behaviour easily.
 */
@MicronautTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    static String query() {
        return "MATCH (p:Person) \n" +
                "RETURN p";

    }

    @Container
    private final static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"));

    @BeforeAll
    void initDatabase() {
        // Retrieve the Bolt URL from the container
        String boltUrl = neo4jContainer.getBoltUrl();
        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword())); Session session = driver.session()) {
            session.run("CREATE (p:Person {" +
                    "name: 'aDeveloper', " +
                    "friends: ['otherDevelopers', 'PO', 'otherQas']" +
                    "})");
            session.run("CREATE (p:Person {" +
                    "name: 'aQa', " +
                    "friends: ['otherQas', 'otherDevelopers']" +
                    "})");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetch() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of(
            "query", query(),
            "url", neo4jContainer.getBoltUrl(),
            "username", "neo4j",
            "password", neo4jContainer.getAdminPassword(),
            "flow", ImmutableMap.of("id", 1, "namespace", "io.kestra.tests"),
            "execution", ImmutableMap.of("id", 1),
            "taskrun", ImmutableMap.of("id", 1)
        ));

        Query query = Query.builder()
            .query("{{query}}")
            .url("{{url}}")
            .username("{{username}}")
            .password("{{password}}")
            .fetch(true)
            .build();

        Query.Output run = query.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(2));

        assertThat(rows.get(0).get("name"), is("aDeveloper"));
        assertThat((List<String>) rows.get(0).get("friends"), containsInAnyOrder("otherDevelopers", "PO", "otherQas"));
        assertThat(rows.get(1).get("name"), is("aQa"));
        assertThat((List<String>) rows.get(1).get("friends"), containsInAnyOrder("otherQas", "otherDevelopers"));
    }
    @Test
    @SuppressWarnings("unchecked")
    void fetchOne() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of(
            "query", query(),
            "url", neo4jContainer.getBoltUrl(),
            "username", "neo4j",
            "password", neo4jContainer.getAdminPassword(),
            "flow", ImmutableMap.of("id", 1, "namespace", "io.kestra.tests"),
            "execution", ImmutableMap.of("id", 1),
            "taskrun", ImmutableMap.of("id", 1)
        ));

        Query query = Query.builder()
            .query("{{query}}")
            .url("{{url}}")
            .username("{{username}}")
            .password("{{password}}")
            .fetchOne(true)
            .build();

        Query.Output run = query.run(runContext);

        Map<String, Object> row = run.getRow();

        assertThat(row.get("name"), is("aDeveloper"));
        assertThat((List<String>) row.get("friends"), containsInAnyOrder("otherDevelopers", "PO", "otherQas"));
    }
    @Test
    @SuppressWarnings("unchecked")
    void store() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of(
            "query", query(),
            "url", neo4jContainer.getBoltUrl(),
            "username", "neo4j",
            "password", neo4jContainer.getAdminPassword(),
            "flow", ImmutableMap.of("id", 1, "namespace", "io.kestra.tests"),
            "execution", ImmutableMap.of("id", 1),
            "taskrun", ImmutableMap.of("id", 1)
        ));

        Query query = Query.builder()
            .query("{{query}}")
            .url("{{url}}")
            .username("{{username}}")
            .password("{{password}}")
            .store(true)
            .build();

        Query.Output run = query.run(runContext);

        assertThat(run.getSize(), is(2));
    }
}