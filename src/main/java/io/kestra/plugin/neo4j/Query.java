package io.kestra.plugin.neo4j;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@Schema(
    title = "Execute a query to a neo4j database."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: \"{{url}}\"",
                "username: \"{{username}}\"",
                "password: \"{{password}}\"",
                "query: |",
                "   MATCH (p:Person)",
                "   RETURN p",
                "storeType: FETCH"
            }
        )
    }
)
public class Query extends AbstractNeo4jConnection implements RunnableTask<Query.Output> {
    @Schema(
        title = "The Neo4J query to perform"
    )
    @PluginProperty(dynamic = true)
    private String query;

    @Schema(
        title = "The way you want to store the data",
        description = "FETCHONE output the first row"
            + "FETCH output all the row"
            + "STORE store all row in a file"
            + "NONE do nothing"
    )
    @Builder.Default
    @PluginProperty
    private StoreType storeType = StoreType.NONE;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (Driver driver = GraphDatabase.driver(runContext.render(getUrl()), this.credentials(runContext)); Session session = driver.session()) {
            Output.OutputBuilder output = Output.builder();

            String render = runContext.render(query);
            logger.warn("Starting query: {}", render);
            Result result = session.run(render);

            switch (storeType) {
                case STORE:
                    Map.Entry<URI, Long> store = this.storeResult(result, runContext);
                    runContext.metric(Counter.of("store.size", store.getValue()));
                    output
                        .uri(store.getKey())
                        .size(store.getValue());
                    break;
                case FETCH: {
                    List<Map<String, Object>> fetchedResult = this.fetchResult(result);
                    output.rows(fetchedResult);
                    output.size((long) fetchedResult.size());
                    runContext.metric(Counter.of("store.size", fetchedResult.size()));
                    break;
                }
                case FETCHONE: {
                    List<Map<String, Object>> fetchedResult = this.fetchResult(result);
                    output.row(fetchedResult.size() > 0 ? fetchedResult.get(0) : ImmutableMap.of());
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
            title = "List containing the fetched data",
            description = "Only populated if using `FETCH`."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "Map containing the first row of fetched data",
            description = "Only populated if using `FETCHONE`."
        )
        private Map<String, Object> row;

        @Schema(
            title = "The uri of the stored result",
            description = "Only populated if using `STORE`"
        )
        private URI uri;

        @Schema(
            title = "The count of the rows fetch"
        )
        private Long size;
    }

    private Map.Entry<URI, Long> storeResult(Result result, RunContext runContext) throws IOException {
        // temp file
        File tempFile = runContext.tempFile(".ion").toFile();

        try (
            OutputStream output = new FileOutputStream(tempFile)
        ) {
            Flux<Object> flowable = Flux
                .create(
                    s -> {
                        StreamSupport
                            .stream(
                                result
                                    .stream()
                                    .map(Record::values)
                                    .flatMap(Collection::stream)
                                    .map(Value::asMap).spliterator(),
                                false
                            )
                            .forEach(s::next);

                        s.complete();
                    },
                    FluxSink.OverflowStrategy.BUFFER
                )
                .doOnNext(throwConsumer(row -> FileSerde.write(output, row)));

            // metrics & finalize
            Mono<Long> count = flowable.count();
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
            .map(Record::values)
            .flatMap(Collection::stream)
            .map(Value::asMap)
            .collect(Collectors.toList());
    }
}
