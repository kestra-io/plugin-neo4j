package io.kestra.plugin.neo4j;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

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
    private static final Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"));

    @BeforeAll
    void initDatabase() {
        try (
            Driver driver = GraphDatabase.driver(
                neo4jContainer.getBoltUrl(),
                AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword())
            );
            Session session = driver.session()
        ) {
            session.run("CREATE (:Team {name: 'core'})").consume();
        }
    }

    @Test
    void commitsAllStatementsAndReturnsCounters() throws Exception {
        Transaction transaction = Transaction.builder()
            .id(IdUtils.create())
            .type(Transaction.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .statements(Property.ofValue(List.of(
                Transaction.Statement.builder()
                    .query(Property.ofValue("CREATE (:Person {name: $name})"))
                    .parameters(Property.ofValue(Map.of("name", "Sanjeev")))
                    .build(),
                Transaction.Statement.builder()
                    .query(Property.ofValue("CREATE (:Person {name: $name})"))
                    .parameters(Property.ofValue(Map.of("name", "Kestra")))
                    .build()
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, ImmutableMap.of());
        Transaction.Output output = transaction.run(runContext);

        assertThat(output.getNodesCreated(), is(2L));
        assertThat(output.getPropertiesSet(), is(2L));

        try (
            Driver driver = GraphDatabase.driver(
                neo4jContainer.getBoltUrl(),
                AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword())
            );
            Session session = driver.session()
        ) {
            long count = session.run("MATCH (p:Person {name: 'Sanjeev'}) RETURN count(p) AS count")
                .single()
                .get("count")
                .asLong();
            assertThat(count, is(1L));
        }
    }

    @Test
    void rollsBackAllStatementsWhenOneFails() throws Exception {
        String marker = "rollback-" + IdUtils.create();

        Transaction transaction = Transaction.builder()
            .id(IdUtils.create())
            .type(Transaction.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .statements(Property.ofValue(List.of(
                Transaction.Statement.builder()
                    .query(Property.ofValue("CREATE (:RollbackMarker {id: $id})"))
                    .parameters(Property.ofValue(Map.of("id", marker)))
                    .build(),
                Transaction.Statement.builder()
                    .query(Property.ofValue("MATCH (p:DefinitelyInvalid) RETURN p"))
                    .build()
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, ImmutableMap.of());

        assertThrows(ClientException.class, () -> transaction.run(runContext));

        try (
            Driver driver = GraphDatabase.driver(
                neo4jContainer.getBoltUrl(),
                AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword())
            );
            Session session = driver.session()
        ) {
            long count = session.run("MATCH (p:RollbackMarker {id: $id}) RETURN count(p) AS count", Map.of("id", marker))
                .single()
                .get("count")
                .asLong();
            assertThat(count, is(0L));
        }
    }
}
