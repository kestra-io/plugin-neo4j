package io.kestra.plugin.neo4j;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.TypeSystem;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.neo4j.models.StoreType;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@Schema(
    title = "Execute a Neo4j Cypher query",
    description = "Runs a rendered Cypher statement on a Neo4j database and handles results as fetch, fetch-one, store to internal storage, or no-op (default NONE)."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: neo4j_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.neo4j.Query
                    url: "{{ url }}"
                    username: "{{ username }}"
                    password: "{{ password }}"
                    query: |
                        MATCH (p:Person)
                        RETURN p
                    storeType: FETCH
                """
        ),
        @Example(
            full = true,
            code = """
                id: neo4j_query_private_ca
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.neo4j.Query
                    url: "bolt://localhost:7687"
                    username: "{{ secret('NEO4J_USERNAME') }}"
                    password: "{{ secret('NEO4J_PASSWORD') }}"
                    encryption: true
                    trustStrategy: CUSTOM
                    trustedCertificate: "{{ secret('NEO4J_CA_PEM') }}"
                    query: |
                        MATCH (p:Person)
                        RETURN p
                    storeType: FETCH
                """
        ),
        @Example(
            full = true,
            code = """
                id: neo4j_query_read
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.neo4j.Query
                    url: "{{ url }}"
                    username: "{{ secret('NEO4J_USERNAME') }}"
                    password: "{{ secret('NEO4J_PASSWORD') }}"
                    accessMode: READ
                    query: |
                        MATCH (p:Person)
                        RETURN p
                    storeType: FETCHONE
                """
        )
    },
    metrics = {
        @Metric(
            name = "store.size",
            type = Counter.TYPE,
            description = "The number of records stored in STORE mode."
        ),
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            description = "The number of records fetched in FETCH or FETCHONE mode."
        )
    }
)
public class Query extends AbstractNeo4jConnection implements RunnableTask<Query.Output> {
    @Schema(
        title = "Cypher query to run",
        description = "Rendered with Flow variables before execution; must be valid for the chosen result mode."
    )
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "Result handling mode",
        description = "FETCHONE returns the first row; FETCH returns all rows; STORE writes all rows to internal storage; NONE skips result handling (default)."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<StoreType> storeType = Property.ofValue(StoreType.NONE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (Driver driver = this.buildDriver(runContext); Session session = this.openSession(driver, runContext)) {
            Output.OutputBuilder output = Output.builder();

            String render = runContext.render(query).as(String.class).orElse(null);
            logger.debug("Starting query: {}", render);
            Result result = session.run(render);

            switch (runContext.render(storeType).as(StoreType.class).orElseThrow()) {
                case STORE: {
                    Map.Entry<URI, Long> store = this.storeResult(result, runContext);
                    runContext.metric(Counter.of("store.size", store.getValue()));
                    output
                        .uri(store.getKey())
                        .size(store.getValue());
                    break;
                }
                case FETCH: {
                    List<Map<String, Object>> fetchedResult = this.fetchResult(result);
                    output.rows(fetchedResult);
                    output.size((long) fetchedResult.size());
                    runContext.metric(Counter.of("fetch.size", fetchedResult.size()));
                    break;
                }
                case FETCHONE: {
                    List<Map<String, Object>> fetchedResult = this.fetchResult(result);
                    output.row(!fetchedResult.isEmpty() ? fetchedResult.getFirst() : ImmutableMap.of());
                    output.size((long) fetchedResult.size());
                    runContext.metric(Counter.of("fetch.size", fetchedResult.size()));
                    break;
                }
            }

            return output.build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Fetched rows",
            description = "Populated when storeType is `FETCH`. One map per record, keyed by the column names of the `RETURN` clause (e.g. `RETURN p` gives `rows[0].p.name`); nodes and relationships are exposed as their properties."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First fetched row",
            description = "Populated when storeType is `FETCHONE`. A map keyed by the column names of the `RETURN` clause (e.g. `RETURN count(n) AS total` gives `row.total`)."
        )
        private Map<String, Object> row;

        @Schema(
            title = "Stored result URI",
            description = "Populated when storeType is `STORE`; points to internal storage."
        )
        private URI uri;

        @Schema(
            title = "Row count",
            description = "Number of rows fetched or stored."
        )
        private Long size;
    }

    private Map.Entry<URI, Long> storeResult(Result result, RunContext runContext) throws IOException {
        // temp file
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (
            var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            Flux<Object> flowable = Flux
                .create(
                    s ->
                    {
                        StreamSupport
                            .stream(
                                result
                                    .stream()
                                    .map(Query::toMap).spliterator(),
                                false
                            )
                            .forEach(s::next);

                        s.complete();
                    },
                    FluxSink.OverflowStrategy.BUFFER
                );

            Mono<Long> count = FileSerde.writeAll(output, flowable);

            // metrics & finalize
            Long lineCount = count.block();

            output.flush();

            return new AbstractMap.SimpleEntry<>(
                runContext.storage().putFile(tempFile),
                lineCount
            );
        }
    }

    private List<Map<String, Object>> fetchResult(Result result) {
        return result.stream()
            .map(Query::toMap)
            .collect(Collectors.toList());
    }

    private static Map<String, Object> toMap(Record record) {
        return record.asMap(Query::toPlainValue);
    }

    // converts a driver value to plain Java types: nodes and relationships become their property maps,
    // paths become their nodes and relationships, lists and maps are converted recursively
    private static Object toPlainValue(Value value) {
        TypeSystem types = TypeSystem.getDefault();

        if (value.hasType(types.NODE())) {
            return value.asNode().asMap(Query::toPlainValue);
        }
        if (value.hasType(types.RELATIONSHIP())) {
            return value.asRelationship().asMap(Query::toPlainValue);
        }
        if (value.hasType(types.PATH())) {
            Path path = value.asPath();
            return Map.of(
                "nodes", StreamSupport.stream(path.nodes().spliterator(), false)
                    .map(node -> node.asMap(Query::toPlainValue))
                    .toList(),
                "relationships", StreamSupport.stream(path.relationships().spliterator(), false)
                    .map(relationship -> relationship.asMap(Query::toPlainValue))
                    .toList()
            );
        }
        if (value.hasType(types.LIST())) {
            return value.asList(Query::toPlainValue);
        }
        if (value.hasType(types.MAP())) {
            return value.asMap(Query::toPlainValue);
        }
        if (value.hasType(types.POINT())) {
            Point point = value.asPoint();
            return Double.isNaN(point.z())
                ? Map.of("srid", point.srid(), "x", point.x(), "y", point.y())
                : Map.of("srid", point.srid(), "x", point.x(), "y", point.y(), "z", point.z());
        }
        if (value.hasType(types.DURATION())) {
            return value.asIsoDuration().toString();
        }

        return value.asObject();
    }
}
