
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

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
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

        try (
            Driver driver = GraphDatabase.driver(
                runContext.render(getUrl()).as(String.class).orElse(null),
                this.credentials(runContext)
            );
            Session session = driver.session()
        ) {
            Output.OutputBuilder output = Output.builder();

            String renderedQuery = runContext.render(query).as(String.class).orElse(null);
            logger.debug("Starting Neo4j query: {}", renderedQuery);

            Result result = session.run(renderedQuery);

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
                    output
                        .rows(fetchedResult)
                        .size((long) fetchedResult.size());
                    runContext.metric(Counter.of("fetch.size", fetchedResult.size()));
                    break;
                }
                case FETCHONE: {
                    List<Map<String, Object>> fetchedResult = this.fetchResult(result);
                    output
                        .row(!fetchedResult.isEmpty() ? fetchedResult.getFirst() : ImmutableMap.of())
                        .size((long) fetchedResult.size());
                    runContext.metric(Counter.of("fetch.size", fetchedResult.size()));
                    break;
                }
                case NONE: {
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
            description = "Populated when storeType is `FETCH`."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First fetched row",
            description = "Populated when storeType is `FETCHONE`."
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

    private Object convertValue(Value value) {
        Object object = value.asObject();

        if (object instanceof Node node) {
            return node.asMap();
        }

        if (object instanceof Relationship relationship) {
            return relationship.asMap();
        }

        return object;
    }

    private Map<String, Object> convertRecord(Record record) {
        // Preserve the existing behavior for a query returning one node:
        // RETURN p -> {name: ..., friends: ...}
        if (record.size() == 1) {
            Object object = record.get(0).asObject();

            if (object instanceof Node node) {
                return node.asMap();
            }

            if (object instanceof Relationship relationship) {
                return relationship.asMap();
            }
        }

        // For multiple columns or scalar values, preserve column names.
        return record.asMap(this::convertValue);
    }

    private Map.Entry<URI, Long> storeResult(Result result, RunContext runContext) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (
            var output = new BufferedOutputStream(
                new FileOutputStream(tempFile),
                FileSerde.BUFFER_SIZE
            )
        ) {
            Flux<Object> flowable = Flux.create(
                sink ->
                {
                    result.stream()
                        .map(this::convertRecord)
                        .forEach(sink::next);

                    sink.complete();
                },
                FluxSink.OverflowStrategy.BUFFER
            );

            Mono<Long> count = FileSerde.writeAll(output, flowable);
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
            .map(this::convertRecord)
            .collect(Collectors.toList());
    }
}
