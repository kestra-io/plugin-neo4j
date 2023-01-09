package io.kestra.plugin.neo4j;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.*;

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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@Schema(
    title = "Fetch rows matching the given query"
)
public class Query extends AbstractNeo4jConnection implements RunnableTask<Query.Output> {
    @Schema(
        title = "The Neo4J query to perform"
    )
    @PluginProperty(dynamic = true)
    private String query;
    @Schema(
        title = "Fetch the result"
    )
    @Builder.Default
    private boolean fetch = false;

    @Schema(
        title = "Fetch one row"
    )
    @Builder.Default
    private boolean fetchOne = false;
    @Schema(
        title = "Store to Kestra storage"
    )
    @Builder.Default
    private boolean store = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (Driver driver = GraphDatabase.driver(runContext.render(getUrl()), this.credentials(runContext)); Session session = driver.session()) {
            Output.OutputBuilder output = Output.builder();
            Result result = session.run(runContext.render(query));
            if (fetch || fetchOne || store) {
                if (store) {
                    Map.Entry<URI, Long> store = this.storeResult(result, runContext);
//                    runContext.metric(Counter.of("store.size", store.getValue()));
                    output
                        .uri(store.getKey())
                        .size(store.getValue().intValue());
                } else if (fetch || fetchOne) {
                    List<Map<String, Object>> fetchedResult = result.stream()
                        .map(Record::values)
                        .flatMap(Collection::stream)
                        .map(Value::asMap)
                        .collect(Collectors.toList());
                    if (fetch) {
                        output.rows(fetchedResult);
                        output.size(fetchedResult.size());
//                        runContext.metric(Counter.of("store.size", fetchedResult.size()));
                    } else {
                        output.row(fetchedResult.size() > 0 ? fetchedResult.get(0) : ImmutableMap.of());
                        output.size(1);
//                        runContext.metric(Counter.of("fetch.size", 1));
                    }
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
            description = "Only populated if 'fetch' parameter is set to true."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "Map containing the first row of fetched data",
            description = "Only populated if 'fetchOne' parameter is set to true."
        )
        private Map<String, Object> row;

        @Schema(
            title = "The uri of store result",
            description = "Only populated if 'store' is set to true."
        )
        private URI uri;

        @Schema(
            title = "The size of the rows fetch"
        )
        private Integer size;
    }

    private Map.Entry<URI, Long> storeResult(Result result, RunContext runContext) throws IOException {
        // temp file
        File tempFile = runContext.tempFile(".ion").toFile();

        try (
            OutputStream output = new FileOutputStream(tempFile)
        ) {
            Flowable<Object> flowable = Flowable
                .create(
                    s -> {
                        StreamSupport
                            .stream(result.stream()
                                .map(Record::values)
                                .flatMap(Collection::stream)
                                .map(Value::asMap).spliterator(), false)
                            .forEach(fieldValues -> {
                                s.onNext(fieldValues);
                            });

                        s.onComplete();
                    },
                    BackpressureStrategy.BUFFER
                )
                .doOnNext(row -> FileSerde.write(output, row));

            // metrics & finalize
            Single<Long> count = flowable.count();
            Long lineCount = count.blockingGet();

            output.flush();

            return new AbstractMap.SimpleEntry<>(
                runContext.putTempFile(tempFile),
                lineCount
            );
        }
    }

}
