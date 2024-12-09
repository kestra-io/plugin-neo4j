package io.kestra.plugin.neo4j;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface Neo4jConnectionInterface {
    @Schema(
        title = "The URL to a Neo4j instance",
        description = "The URL can either be in HTTP or Bolt format"
    )
    Property<String> getUrl();
}
