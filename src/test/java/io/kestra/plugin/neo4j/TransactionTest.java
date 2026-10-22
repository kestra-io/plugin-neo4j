package io.kestra.plugin.neo4j;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.Neo4jException;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.neo4j.models.Statement;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransactionTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Container
    private final static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"));

    @Test
    void commitAndCounters() throws Exception {
        Transaction transaction = Transaction.builder()
            .id(IdUtils.create())
            .type(Transaction.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .statements(Property.ofValue(List.of(
                statement("CREATE (p:TransactionAlice {name: 'Alice', team: 'core'})", null),
                statement("CREATE (p:TransactionBob {name: 'Bob', team: 'qa'})", null),
                statement("MATCH (a:TransactionAlice), (b:TransactionBob) CREATE (a)-[:KNOWS]->(b)", null)
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());
        Transaction.Output output = transaction.run(runContext);

        assertThat(output.getStatements(), is(3L));
        assertThat(output.getCounters().getNodesCreated(), is(2L));
        assertThat(output.getCounters().getRelationshipsCreated(), is(1L));
        assertThat(output.getCounters().getPropertiesSet(), is(4L));
        assertThat(output.getCounters().getNodesDeleted(), is(0L));
        assertThat(output.getResultAvailableAfter() >= 0L, is(true));

        try (Driver driver = driver(); Session session = driver.session()) {
            long nodes = session.run("MATCH (n) WHERE n:TransactionAlice OR n:TransactionBob RETURN count(n) AS c")
                .single()
                .get("c")
                .asLong();
            long relationships = session.run("MATCH (:TransactionAlice)-[r:KNOWS]->(:TransactionBob) RETURN count(r) AS c")
                .single()
                .get("c")
                .asLong();

            assertThat(nodes, is(2L));
            assertThat(relationships, is(1L));
        }
    }

    @Test
    void rollbackOnFailure() throws Exception {
        Transaction transaction = Transaction.builder()
            .id(IdUtils.create())
            .type(Transaction.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .statements(Property.ofValue(List.of(
                statement("CREATE (p:RollbackPerson {name: 'Ghost'})", null),
                statement("MATCH p:Invalid RETURN p", null)
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());

        assertThrows(Neo4jException.class, () -> transaction.run(runContext));

        try (Driver driver = driver(); Session session = driver.session()) {
            long created = session.run("MATCH (p:RollbackPerson) RETURN count(p) AS c")
                .single()
                .get("c")
                .asLong();

            // the first statement must have been rolled back with the failing one
            assertThat(created, is(0L));
        }
    }

    @Test
    void bindsParameters() throws Exception {
        Transaction transaction = Transaction.builder()
            .id(IdUtils.create())
            .type(Transaction.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .statements(Property.ofValue(List.of(
                statement(
                    "CREATE (p:BoundPerson {name: $name, team: $team})",
                    Map.of("name", "Carol", "team", "product")
                )
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());
        Transaction.Output output = transaction.run(runContext);

        assertThat(output.getCounters().getNodesCreated(), is(1L));
        assertThat(output.getCounters().getPropertiesSet(), is(2L));

        try (Driver driver = driver(); Session session = driver.session()) {
            String name = session.run("MATCH (p:BoundPerson) RETURN p.name AS name")
                .single()
                .get("name")
                .asString();

            assertThat(name, is("Carol"));
        }
    }

    @Test
    void failsOnEmptyStatements() {
        Transaction transaction = Transaction.builder()
            .id(IdUtils.create())
            .type(Transaction.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .statements(Property.ofValue(List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());

        assertThrows(IllegalArgumentException.class, () -> transaction.run(runContext));
    }

    private static Statement statement(String query, Map<String, Object> parameters) {
        Statement.StatementBuilder builder = Statement.builder()
            .query(Property.ofValue(query));

        if (parameters != null) {
            builder.parameters(Property.ofValue(parameters));
        }

        return builder.build();
    }

    private Driver driver() {
        return GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword()));
    }
}
