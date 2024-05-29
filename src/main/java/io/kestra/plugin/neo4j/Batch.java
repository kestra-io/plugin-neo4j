package io.kestra.plugin.neo4j;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.neo4j.driver.*;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@Schema(
    title = "Execute a batch query to a Neo4j database."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: \"{{url}}\"",
                "username: \"{{username}}\"",
                "password: \"{{password}}\"",
                "query: |",
                "   UNWIND $props AS properties",
                "   MERGE (y:Year {year: properties.year})",
                "   MERGE (y)<-[:IN]-(e:Event {id: properties.id})\n",
                "   RETURN e.id AS x ORDER BY x\n",
                "from: \"{{ outputs['previous-task-id'].uri }}\"",
                "chunk: 1000"
            }
        )
    }
)
public class Batch extends AbstractNeo4jConnection implements RunnableTask<Batch.Output>, Neo4jConnectionInterface {
    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @NotNull
    @Schema(
        title = "Query to execute batch, must use UNWIND",
        description = "The query must have the row :"
            + "\n\"UNWIND $props AS X\" with $props the variable where"
            + "\n we input the source data for the batch."
    )
    @PluginProperty(dynamic = true)
    private String query;

    @Schema(
        title = "The size of chunk for every bulk request"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    @NotNull
    private Integer chunk = 1000;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (Driver driver = GraphDatabase.driver(runContext.render(getUrl()), this.credentials(runContext)); Session session = driver.session()) {
            Logger logger = runContext.logger();
            String query = runContext.render(this.query);
            URI from = new URI(runContext.render(this.from));
            Transaction tx = session.beginTransaction();

            logger.debug("Starting query: {}", query);

            try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)))) {
                Flux<Integer> flowable;
                AtomicLong count = new AtomicLong();

                flowable = Flux.create(FileSerde.reader(inputStream), FluxSink.OverflowStrategy.BUFFER)
                    .buffer(this.chunk, this.chunk)
                    .map(o -> {
                        Map<String, Object> params = new HashMap<>();
                        params.put("props", o);
                        Result result = tx.run(query, params);
                        int updated = result.list().size();
                        count.incrementAndGet();

                        return updated;
                    });

                Integer updated = flowable.reduce(Integer::sum).block();

                runContext.metric(Counter.of("records", count.get()));
                runContext.metric(Counter.of("updated", updated == null ? 0 : updated));

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
