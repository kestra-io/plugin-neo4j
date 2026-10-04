package io.kestra.plugin.neo4j;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.neo4j.models.StoreType;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@KestraTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    static String query() {
        return "MATCH (p:Person) \n" +
            "RETURN p";
    }

    static String scalarQuery() {
        return "MATCH (p:Person) \n" +
            "RETURN count(p) AS total";
    }

    static String scalarColumnQuery() {
        return "MATCH (p:Person) \n" +
            "RETURN p.name AS name \n" +
            "ORDER BY name";
    }

    static String multipleColumnsQuery() {
        return "MATCH (p:Person) \n" +
            "RETURN p.name AS name, p.friends AS friends \n" +
            "ORDER BY name";
    }

    static String mapColumnQuery() {
        return "RETURN {status: 'ready'} AS health";
    }

    static String nestedNodesQuery() {
        return "MATCH (a:Person {name: 'aDeveloper'}), (b:Person {name: 'aQa'}) \n" +
            "RETURN [a, b] AS people";
    }

    static String pathQuery() {
        return "MATCH p = (a:Person {name: 'aDeveloper'})-[:KNOWS]->(b:Person) \n" +
            "RETURN p";
    }

    static String temporalAndSpatialQuery() {
        return "RETURN point({x: 1.0, y: 2.0}) AS location2d, \n" +
            "point({x: 1.0, y: 2.0, z: 3.0}) AS location3d, \n" +
            "duration('P1DT2H') AS elapsed";
    }

    static String invalidQuery() {
        return "MATCH p:Invalid \n" +
            "RETURN p";
    }

    static Query buildQuery(String query, StoreType storeType) {
        return Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue(query))
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .storeType(Property.ofValue(storeType))
            .build();
    }

    @Container
    private final static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"));

    @BeforeAll
    void initDatabase() {
        // Retrieve the Bolt URL from the container
        String boltUrl = neo4jContainer.getBoltUrl();
        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword())); Session session = driver.session()) {
            session.run(
                "CREATE (p:Person {" +
                    "name: 'aDeveloper', " +
                    "friends: ['otherDevelopers', 'PO', 'otherQas']" +
                    "})"
            );
            session.run(
                "CREATE (p:Person {" +
                    "name: 'aQa', " +
                    "friends: ['otherQas', 'otherDevelopers']" +
                    "})"
            );
            session.run(
                "MATCH (a:Person {name: 'aDeveloper'}), (b:Person {name: 'aQa'}) " +
                    "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
            );
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetch() throws Exception {
        Query query = buildQuery(query(), StoreType.FETCH);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(2));

        Map<String, Object> first = (Map<String, Object>) rows.get(0).get("p");
        Map<String, Object> second = (Map<String, Object>) rows.get(1).get("p");
        assertThat(first.get("name"), is("aDeveloper"));
        assertThat((List<String>) first.get("friends"), containsInAnyOrder("otherDevelopers", "PO", "otherQas"));
        assertThat(second.get("name"), is("aQa"));
        assertThat((List<String>) second.get("friends"), containsInAnyOrder("otherQas", "otherDevelopers"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchOne() throws Exception {
        Query query = buildQuery(query(), StoreType.FETCHONE);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        Map<String, Object> row = (Map<String, Object>) run.getRow().get("p");

        assertThat(row.get("name"), is("aDeveloper"));
        assertThat((List<String>) row.get("friends"), containsInAnyOrder("otherDevelopers", "PO", "otherQas"));
    }

    @Test
    void fetchOneScalar() throws Exception {
        Query query = buildQuery(scalarQuery(), StoreType.FETCHONE);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        assertThat(run.getRow().get("total"), is(2L));
        assertThat(run.getSize(), is(1L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchMultipleColumns() throws Exception {
        Query query = buildQuery(multipleColumnsQuery(), StoreType.FETCH);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        // one map per record, not one entry per column
        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(2));
        assertThat(run.getSize(), is(2L));
        assertThat(rows.get(0).get("name"), is("aDeveloper"));
        assertThat((List<String>) rows.get(0).get("friends"), containsInAnyOrder("otherDevelopers", "PO", "otherQas"));
        assertThat(rows.get(1).get("name"), is("aQa"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchOneMapColumn() throws Exception {
        Query query = buildQuery(mapColumnQuery(), StoreType.FETCHONE);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        assertThat(((Map<String, Object>) run.getRow().get("health")).get("status"), is("ready"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchOneNestedNodes() throws Exception {
        Query query = buildQuery(nestedNodesQuery(), StoreType.FETCHONE);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        // nodes inside a list are converted to their properties too
        List<Map<String, Object>> people = (List<Map<String, Object>>) run.getRow().get("people");
        assertThat(people.size(), is(2));
        assertThat(people.get(0).get("name"), is("aDeveloper"));
        assertThat(people.get(1).get("name"), is("aQa"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchOnePath() throws Exception {
        Query query = buildQuery(pathQuery(), StoreType.FETCHONE);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        // a path keeps both its nodes and its relationships
        Map<String, Object> path = (Map<String, Object>) run.getRow().get("p");
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) path.get("nodes");
        List<Map<String, Object>> relationships = (List<Map<String, Object>>) path.get("relationships");
        assertThat(nodes.size(), is(2));
        assertThat(nodes.get(0).get("name"), is("aDeveloper"));
        assertThat(nodes.get(1).get("name"), is("aQa"));
        assertThat(relationships.size(), is(1));
        assertThat(relationships.get(0).get("since"), is(2020L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchOnePointAndDuration() throws Exception {
        Query query = buildQuery(temporalAndSpatialQuery(), StoreType.FETCHONE);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        Map<String, Object> location2d = (Map<String, Object>) run.getRow().get("location2d");
        assertThat(location2d, is(Map.of("srid", 7203, "x", 1.0, "y", 2.0)));

        Map<String, Object> location3d = (Map<String, Object>) run.getRow().get("location3d");
        assertThat(location3d, is(Map.of("srid", 9157, "x", 1.0, "y", 2.0, "z", 3.0)));

        assertThat(run.getRow().get("elapsed"), is("P0M1DT7200S"));
    }

    @Test
    void store() throws Exception {
        Query query = buildQuery(query(), StoreType.STORE);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        assertThat(run.getSize(), is(2L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void storeScalars() throws Exception {
        Query query = buildQuery(scalarColumnQuery(), StoreType.STORE);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        assertThat(run.getSize(), is(2L));

        List<Object> stored = new ArrayList<>();
        try (InputStream is = new BufferedInputStream(runContext.storage().getFile(run.getUri()), FileSerde.BUFFER_SIZE)) {
            FileSerde.read(is, stored::add);
        }
        assertThat(stored.size(), is(2));
        assertThat(((Map<String, Object>) stored.get(0)).get("name"), is("aDeveloper"));
        assertThat(((Map<String, Object>) stored.get(1)).get("name"), is("aQa"));
    }

    @Test
    void failed() throws Exception {
        Query query = buildQuery(invalidQuery(), StoreType.FETCH);

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());

        assertThrows(ClientException.class, () ->
        {
            query.run(runContext);
        });
    }

    @Test
    void parametersBinding() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("MATCH (p:Person {name: $name}) \n" + "RETURN p"))
            .parameters(Property.ofValue(ImmutableMap.of("name", "aDeveloper")))
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .storeType(Property.ofValue(StoreType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(1));
        assertThat(rows.get(0).get("name"), is("aDeveloper"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void renderedParameters() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("MATCH (p:Person {name: $name}) \n" + "RETURN p"))
            .parameters(new Property<>(ImmutableMap.of("name", "{{ inputs.personName }}")))
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .storeType(Property.ofValue(StoreType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of("personName", "aQa"));
        Query.Output run = query.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(1));
        assertThat(rows.get(0).get("name"), is("aQa"));
        assertThat((List<String>) rows.get(0).get("friends"), containsInAnyOrder("otherQas", "otherDevelopers"));
    }

    @Test
    void nestedMapParameters() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("RETURN $metadata AS metadata"))
            .parameters(new Property<>(ImmutableMap.of("metadata", ImmutableMap.of("department", "{{ inputs.dept }}", "region", "emea"))))
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .storeType(Property.ofValue(StoreType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of("dept", "engineering"));
        Query.Output run = query.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(1));
        assertThat(rows.get(0).get("department"), is("engineering"));
        assertThat(rows.get(0).get("region"), is("emea"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void nestedListParameters() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("RETURN {tags: $tags} AS result"))
            .parameters(new Property<>(ImmutableMap.of("tags", ImmutableList.of("{{ inputs.tag1 }}", "{{ inputs.tag2 }}"))))
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .storeType(Property.ofValue(StoreType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of("tag1", "red", "tag2", "blue"));
        Query.Output run = query.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(1));
        assertThat((List<String>) rows.get(0).get("tags"), containsInAnyOrder("red", "blue"));
    }

    @Test
    void missingParameter() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("MATCH (p:Person {name: $missing}) \n" + "RETURN p"))
            .parameters(Property.ofValue(ImmutableMap.of("name", "aDeveloper")))
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .storeType(Property.ofValue(StoreType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());

        assertThrows(ClientException.class, () ->
        {
            query.run(runContext);
        });
    }

    @Test
    void explicitDatabase() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue(query()))
            .database(Property.ofValue("neo4j"))
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .storeType(Property.ofValue(StoreType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(2));
    }

    @Test
    @SuppressWarnings("unchecked")
    void systemDatabase() throws Exception {
        Query query = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .query(Property.ofValue("SHOW DATABASES YIELD name RETURN {name: name} AS db"))
            .database(Property.ofValue("system"))
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .storeType(Property.ofValue(StoreType.FETCH))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
        Query.Output run = query.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        List<String> names = rows.stream().map(row -> (String) row.get("name")).toList();
        assertThat(names, hasItems("neo4j", "system"));
    }
}
