package io.kestra.plugin.neo4j;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.neo4j.auth.AuthentifiedTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.*;
import org.neo4j.driver.util.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@Schema(
        title = "Fetch rows matching the given query"
)
public class QueryTask extends AuthentifiedTask implements RunnableTask<QueryTask.Output> {
    @Schema(
            title = "The target Neo4J database url",
            description = "The url can either be in HTTP or Bolt format"
    )
    private String url;
    @Schema(
            title = "The Neo4J query to perform"
    )
    @PluginProperty(dynamic = true)
    private String query;
    @Schema(
            title = "Should fetch for results"
    )
    @PluginProperty(dynamic = false)
    private boolean fetch;
    @Schema(
            title = "Should store to Kestra storage"
    )
    @PluginProperty(dynamic = false)
    private boolean store;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (Driver driver = GraphDatabase.driver(runContext.render(url), this.credentials(runContext)); Session session = driver.session()) {
            Output.OutputBuilder builder = Output.builder();
            if (fetch) {
                builder.rows(session.run(runContext.render(query)).stream()
                        .map(Record::values)
                        .flatMap(Collection::stream)
                        .map(Value::asMap)
                        .collect(Collectors.toList()));
            }

            return builder.build();
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
    }
}
