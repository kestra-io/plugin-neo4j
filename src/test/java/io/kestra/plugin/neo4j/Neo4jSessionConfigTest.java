package io.kestra.plugin.neo4j;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class Neo4jSessionConfigTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void defaultSessionConfigWhenDatabaseUnset() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("MATCH (n) RETURN count(n) AS total"))
            .url(Property.ofValue("bolt://localhost:7687"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());

        assertThat(query.sessionConfig(runContext).database().isPresent(), is(false));
    }

    @Test
    void sessionConfigForDatabase() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("MATCH (n) RETURN count(n) AS total"))
            .database(Property.ofValue("analytics"))
            .url(Property.ofValue("bolt://localhost:7687"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());

        assertThat(query.sessionConfig(runContext).database().orElse(null), is("analytics"));
    }

    @Test
    void renderedDatabase() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("MATCH (n) RETURN count(n) AS total"))
            .database(new Property<>("{{ inputs.dbName }}"))
            .url(Property.ofValue("bolt://localhost:7687"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of("dbName", "analytics"));

        assertThat(query.sessionConfig(runContext).database().orElse(null), is("analytics"));
    }
}