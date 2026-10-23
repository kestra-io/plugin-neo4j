package io.kestra.plugin.neo4j.models;

import java.util.Map;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Schema(
    title = "A Cypher statement executed inside the transaction",
    description = "One statement of the `statements` list. All statements of a `Transaction` task run in the same managed transaction, in the order they are declared."
)
public class Statement {
    @NotNull
    @Schema(
        title = "Cypher statement to run",
        description = "Rendered with Flow variables before execution. Use `$name` placeholders and bind their values with `parameters`."
    )
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "Query parameters",
        description = "Values bound to the `$name` placeholders of `query`, for example `id: \"{{ inputs.id }}\"`. A value produced by a Pebble expression is bound as a string; cast it in Cypher, for example with `toInteger($id)`."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> parameters;
}
