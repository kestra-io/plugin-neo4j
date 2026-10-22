package io.kestra.plugin.neo4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
        Transaction transaction = transactionWith(List.of(
            statement("CREATE (p:TransactionAlice {name: 'Alice', team: 'core'})", null),
            statement("CREATE (p:TransactionBob {name: 'Bob', team: 'qa'})", null),
            statement("MATCH (a:TransactionAlice), (b:TransactionBob) CREATE (a)-[:KNOWS]->(b)", null)
        ));

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());
        Transaction.Output output = transaction.run(runContext);

        assertThat(output.getCounters().getNodesCreated(), is(2L));
        assertThat(output.getCounters().getRelationshipsCreated(), is(1L));
        assertThat(output.getCounters().getPropertiesSet(), is(4L));
        assertThat(output.getCounters().getNodesDeleted(), is(0L));
        assertThat(output.getCounters().getLabelsAdded(), is(2L));
        assertThat(output.getCounters().getLabelsRemoved(), is(0L));
        assertThat(output.getResultAvailableAfter(), greaterThanOrEqualTo(0L));

        double createdMetric = runContext.metrics().stream()
            .filter(metric -> "nodes.created".equals(metric.getName()))
            .mapToDouble(metric -> ((Number) metric.getValue()).doubleValue())
            .findFirst()
            .orElseThrow();

        assertThat(createdMetric, is(2.0));

        assertThat(count("MATCH (n) WHERE n:TransactionAlice OR n:TransactionBob RETURN count(n) AS c"), is(2L));
        assertThat(count("MATCH (:TransactionAlice)-[r:KNOWS]->(:TransactionBob) RETURN count(r) AS c"), is(1L));
    }

    @Test
    void rollbackOnRuntimeFailure() throws Exception {
        Transaction transaction = transactionWith(List.of(
            statement("CREATE (p:RollbackPerson {name: 'Ghost'})", null),
            statement("RETURN 1 / 0 AS boom", null)
        ));

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());

        var exception = assertThrows(IllegalStateException.class, () -> transaction.run(runContext));
        assertThat(exception.getMessage(), containsString("Statement 2"));
        assertThat(count("MATCH (p:RollbackPerson) RETURN count(p) AS c"), is(0L));
    }

    @Test
    void bindsParameters() throws Exception {
        Transaction transaction = transactionWith(List.of(
            statement(
                "CREATE (p:BoundPerson {name: $name, team: $team})",
                Map.of("name", "Carol", "team", "product")
            )
        ));

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
    void rendersInputsOnlyOnce() throws Exception {
        Transaction transaction = transactionWith(List.of(
            Statement.builder()
                .query(Property.ofValue("CREATE (p:RenderOnce {name: $name, note: $note})"))
                .parameters(Property.ofExpression("{\"name\": \"{{ inputs.name }}\", \"note\": \"{{ inputs.note }}\"}"))
                .build()
        ));

        RunContext runContext = TestsUtils.mockRunContext(
            runContextFactory,
            transaction,
            Map.of("name", "{{ 1 + 1 }}", "note", "{{ secret('SHOULD_NEVER_BE_EVALUATED') }}")
        );

        transaction.run(runContext);

        try (Driver driver = driver(); Session session = driver.session()) {
            Record record = session.run("MATCH (p:RenderOnce) RETURN p.name AS name, p.note AS note").single();

            assertThat(record.get("name").asString(), is("{{ 1 + 1 }}"));
            assertThat(record.get("note").asString(), is("{{ secret('SHOULD_NEVER_BE_EVALUATED') }}"));
        }
    }

    @Test
    void failsOnBlankQuery() {
        Transaction transaction = transactionWith(List.of(statement("   ", null)));

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());

        var exception = assertThrows(IllegalArgumentException.class, () -> transaction.run(runContext));
        assertThat(exception.getMessage(), containsString("non-blank"));
    }

    @Test
    void rejectsNegativeMaxRetryTime() {
        var transaction = Transaction.builder()
            .id(IdUtils.create())
            .type(Transaction.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .statements(List.of(statement("RETURN 1 AS one", null)))
            .maxRetryTime(Property.ofValue(Duration.ofSeconds(-5)))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());

        assertThrows(IllegalArgumentException.class, () -> transaction.run(runContext));
    }

    @Test
    void failsOnEmptyStatements() {
        Transaction transaction = transactionWith(List.of());

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, transaction, Map.of());

        assertThrows(IllegalArgumentException.class, () -> transaction.run(runContext));
    }

    private Transaction transactionWith(List<Statement> statements) {
        return Transaction.builder()
            .id(IdUtils.create())
            .type(Transaction.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .statements(statements)
            .build();
    }

    private static Statement statement(String query, Map<String, Object> parameters) {
        Statement.StatementBuilder builder = Statement.builder()
            .query(Property.ofValue(query));

        if (parameters != null) {
            builder.parameters(Property.ofValue(parameters));
        }

        return builder.build();
    }

    private long count(String cypher) {
        try (Driver driver = driver(); Session session = driver.session()) {
            return session.run(cypher).single().get("c").asLong();
        }
    }

    private Driver driver() {
        return GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword()));
    }
}
