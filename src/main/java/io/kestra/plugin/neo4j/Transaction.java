package io.kestra.plugin.neo4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.summary.ResultSummary;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@Schema(
    title = "Execute Neo4j statements in a transaction",
    description = "Runs multiple Cypher statements atomically in one managed Neo4j transaction. A failure rolls back the complete transaction, while transient failures are retried by the Neo4j driver."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: neo4j_transaction
                namespace: company.team

                tasks:
                  - id: tx
                    type: io.kestra.plugin.neo4j.Transaction
                    url: "{{ secret('NEO4J_URL') }}"
                    username: "{{ secret('NEO4J_USER') }}"
                    password: "{{ secret('NEO4J_PASSWORD') }}"
                    statements:
                      - query: "CREATE (:Person {id: $id, name: $name})"
                        parameters:
                          id: "{{ inputs.id }}"
                          name: "{{ inputs.name }}"
                      - query: |
                          MATCH (p:Person {id: $id}), (t:Team {name: 'core'})
                          MERGE (p)-[:MEMBER_OF]->(t)
                        parameters:
                          id: "{{ inputs.id }}"
                """
        )
    },
    metrics = {
        @Metric(name = "nodes.created", type = Counter.TYPE, description = "The number of nodes created by the transaction."),
        @Metric(name = "nodes.deleted", type = Counter.TYPE, description = "The number of nodes deleted by the transaction."),
        @Metric(name = "relationships.created", type = Counter.TYPE, description = "The number of relationships created by the transaction."),
        @Metric(name = "relationships.deleted", type = Counter.TYPE, description = "The number of relationships deleted by the transaction."),
        @Metric(name = "properties.set", type = Counter.TYPE, description = "The number of properties set by the transaction."),
        @Metric(name = "labels.added", type = Counter.TYPE, description = "The number of labels added by the transaction."),
        @Metric(name = "labels.removed", type = Counter.TYPE, description = "The number of labels removed by the transaction.")
    }
)
public class Transaction extends AbstractNeo4jConnection implements RunnableTask<Transaction.Output> {
    @NotNull
    @Schema(
        title = "Cypher statements",
        description = "Statements executed in order within one managed transaction. All statements commit together or roll back together."
    )
    @PluginProperty(group = "main")
    private Property<List<Statement>> statements;

    @Schema(
        title = "Maximum transaction retry time",
        description = "Maximum time the Neo4j driver spends retrying transient transaction failures. When omitted, the driver's default is used."
    )
    @PluginProperty(group = "reliability")
    private Property<Duration> maxRetryTime;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String rUrl = runContext.render(getUrl()).as(String.class).orElse(null);
        List<Statement> rStatements = runContext.render(statements).asList(Statement.class);
        Duration rMaxRetryTime = maxRetryTime == null
            ? null
            : runContext.render(maxRetryTime).as(Duration.class).orElse(null);

        AuthToken credentials = credentials(runContext);
        Config config = rMaxRetryTime == null
            ? null
            : Config.builder()
                .withMaxTransactionRetryTime(rMaxRetryTime.toMillis(), TimeUnit.MILLISECONDS)
                .build();

        try (Driver driver = config == null
            ? GraphDatabase.driver(rUrl, credentials)
            : GraphDatabase.driver(rUrl, credentials, config);
             Session session = driver.session()) {

            CounterAccumulator accumulator = new CounterAccumulator();

            TransactionCallback<Void> work = tx -> {
                for (Statement statement : rStatements) {
                    String rQuery = runContext.render(statement.getQuery()).as(String.class).orElse("");
                    Map<String, Object> rParameters = statement.getParameters() == null
                        ? Map.of()
                        : runContext.render(statement.getParameters()).asMap(String.class, Object.class);

                    logger.debug("Executing transaction statement: {}", rQuery);

                    ResultSummary summary = tx.run(rQuery, rParameters).consume();
                    accumulator.add(summary);
                }

                return null;
            };

            session.executeWrite(work);

            runContext.metric(Counter.of("nodes.created", accumulator.nodesCreated));
            runContext.metric(Counter.of("nodes.deleted", accumulator.nodesDeleted));
            runContext.metric(Counter.of("relationships.created", accumulator.relationshipsCreated));
            runContext.metric(Counter.of("relationships.deleted", accumulator.relationshipsDeleted));
            runContext.metric(Counter.of("properties.set", accumulator.propertiesSet));
            runContext.metric(Counter.of("labels.added", accumulator.labelsAdded));
            runContext.metric(Counter.of("labels.removed", accumulator.labelsRemoved));

            return Output.builder()
                .nodesCreated(accumulator.nodesCreated)
                .nodesDeleted(accumulator.nodesDeleted)
                .relationshipsCreated(accumulator.relationshipsCreated)
                .relationshipsDeleted(accumulator.relationshipsDeleted)
                .propertiesSet(accumulator.propertiesSet)
                .labelsAdded(accumulator.labelsAdded)
                .labelsRemoved(accumulator.labelsRemoved)
                .resultAvailableAfter(accumulator.resultAvailableAfter)
                .build();
        }
    }

    private static class CounterAccumulator {
        private long nodesCreated;
        private long nodesDeleted;
        private long relationshipsCreated;
        private long relationshipsDeleted;
        private long propertiesSet;
        private long labelsAdded;
        private long labelsRemoved;
        private long resultAvailableAfter;

        private void add(ResultSummary summary) {
            var counters = summary.counters();
            nodesCreated += counters.nodesCreated();
            nodesDeleted += counters.nodesDeleted();
            relationshipsCreated += counters.relationshipsCreated();
            relationshipsDeleted += counters.relationshipsDeleted();
            propertiesSet += counters.propertiesSet();
            labelsAdded += counters.labelsAdded();
            labelsRemoved += counters.labelsRemoved();
            resultAvailableAfter += summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Nodes created")
        private final Long nodesCreated;

        @Schema(title = "Nodes deleted")
        private final Long nodesDeleted;

        @Schema(title = "Relationships created")
        private final Long relationshipsCreated;

        @Schema(title = "Relationships deleted")
        private final Long relationshipsDeleted;

        @Schema(title = "Properties set")
        private final Long propertiesSet;

        @Schema(title = "Labels added")
        private final Long labelsAdded;

        @Schema(title = "Labels removed")
        private final Long labelsRemoved;

        @Schema(title = "Time until results became available in milliseconds")
        private final Long resultAvailableAfter;
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class Statement {
        @NotNull
        @Schema(title = "Cypher query")
        @PluginProperty(group = "main")
        private Property<String> query;

        @Schema(title = "Parameters passed to the Cypher query")
        @PluginProperty(group = "main")
        private Property<Map<String, Object>> parameters;
    }
}
