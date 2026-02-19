package io.kestra.plugin.neo4j;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface Neo4jConnectionInterface {
    @Schema(
        title = "Neo4j endpoint URL",
        description = "Bolt or HTTP(S) URI used to open the driver connection."
    )
    Property<String> getUrl();
}
