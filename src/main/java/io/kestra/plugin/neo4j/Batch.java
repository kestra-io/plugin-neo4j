package io.kestra.plugin.neo4j;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@Schema(
    title = "Run Cypher batch with UNWIND",
    description = "Reads records from internal storage, binds them to an UNWIND Cypher statement, and executes in a single transaction using chunked batches (default 1000). Ensure the query is idempotent."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: neo4j_batch
                namespace: company.team

                tasks:
                  - id: batch
                    type: io.kestra.plugin.neo4j.Batch
                    url: "{{ url }}"
                    username: "{{ username }}"
                    password: "{{ password }}"
                    query: |
                       UNWIND $props AS properties
                       MERGE (y:Year {year: properties.year})
                       MERGE (y)<-[:IN]-(e:Event {id: properties.id})\n
                       RETURN e.id AS x ORDER BY x\n
                    from: "{{ outputs.previous_task_id.uri }}"
                    chunk: 1000
                """
        )
    },
    metrics = {
        @Metric(
            name = "records.processed",
            type = Counter.TYPE,
            description = "The total number of records processed in the batch."
        ),
        @Metric(
            name = "records.updated",
            type = Counter.TYPE,
            description = "The total number of records updated in the batch."
        )
    }
)
public class Batch extends AbstractNeo4jConnection implements RunnableTask<Batch.Output>, Neo4jConnectionInterface {
    @NotNull
    @Schema(
        title = "Source file URI",
        description = "Internal storage URI (e.g. `kestra://...`) containing JSON lines to stream into the batch."
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @NotNull
    @Schema(
        title = "Cypher UNWIND statement",
        description = "Must include `UNWIND $props AS ...`; `$props` is populated from each chunk of the source file. Rendered with Flow variables before execution."
    )
    private Property<String> query;

    @Schema(
        title = "Chunk size per request",
        description = "Number of records sent in each bulk call (default 1000). Lower to reduce memory use; raise to speed up large imports."
    )
    @Builder.Default
    @NotNull
    private Property<Integer> chunk = Property.ofValue(1000);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (Driver driver = GraphDatabase.driver(runContext.render(getUrl()).as(String.class).orElse(null), this.credentials(runContext)); Session session = driver.session()) {
            Logger logger = runContext.logger();
            String query = runContext.render(this.query).as(String.class).orElseThrow();
            URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());
            Transaction tx = session.beginTransaction();

            logger.debug("Starting query: {}", query);

            try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE)) {
                Flux<Integer> flowable;
                AtomicLong count = new AtomicLong();

                var chunkValue = runContext.render(this.chunk).as(Integer.class).orElseThrow();
                flowable = FileSerde.readAll(inputStream)
                    .buffer(chunkValue, chunkValue)
                    .map(o -> {
                        Map<String, Object> params = new HashMap<>();
                        params.put("props", o);
                        Result result = tx.run(query, params);
                        int updated = result.list().size();
                        count.incrementAndGet();

                        return updated;
                    });

                Integer updated = flowable.reduce(Integer::sum).block();

                runContext.metric(Counter.of("records.processed", count.get(), "origin", "Batch"));
                runContext.metric(Counter.of("records.updated", updated == null ? 0 : updated, "origin", "Batch"));

                logger.info("Successfully bulk {} queries with {} updated rows", count.get(), updated);

                tx.commit();

                return Output
                    .builder()
                    .rowCount(count.get())
                    .updatedCount(updated)
                    .build();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The count of executed queries")
        private final Long rowCount;

        @Schema(title = "The updated rows count")
        private final Integer updatedCount;
    }

}
