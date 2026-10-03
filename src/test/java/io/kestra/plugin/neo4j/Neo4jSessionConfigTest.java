package io.kestra.plugin.neo4j;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
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

    @Test
    @SuppressWarnings("unchecked")
    void nestedParametersRendering() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("MATCH (p:Person {name: $name}) RETURN p"))
            .parameters(
                new Property<>(
                    ImmutableMap.of(
                        "name",
                        "{{ inputs.who }}",
                        "metadata",
                        ImmutableMap.of("department", "{{ inputs.dept }}", "region", "emea"),
                        "tags",
                        ImmutableList.of("{{ inputs.tag1 }}", "{{ inputs.tag2 }}")
                    )
                )
            )
            .url(Property.ofValue("bolt://localhost:7687"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(
            runContextFactory,
            query,
            ImmutableMap.of("who", "aDeveloper", "dept", "engineering", "tag1", "red", "tag2", "blue")
        );

        Map<String, Object> rendered = runContext.render(query.getParameters()).asMap(String.class, Object.class);

        assertThat(rendered.get("name"), is("aDeveloper"));

        Map<String, Object> metadata = (Map<String, Object>) rendered.get("metadata");
        assertThat(metadata.get("department"), is("engineering"));
        assertThat(metadata.get("region"), is("emea"));

        assertThat((List<String>) rendered.get("tags"), is(List.of("red", "blue")));
    }

    @Test
    void emptyParametersWhenUnset() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("MATCH (n) RETURN count(n) AS total"))
            .url(Property.ofValue("bolt://localhost:7687"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());

        assertThat(runContext.render(query.getParameters()).asMap(String.class, Object.class).isEmpty(), is(true));
    }
}
