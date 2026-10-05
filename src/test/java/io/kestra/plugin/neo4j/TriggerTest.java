package io.kestra.plugin.neo4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.neo4j.models.StoreType;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@KestraTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Container
    private static final Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"));

    private Trigger createTrigger(String cypher, StoreType storeType) {
        return Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .url(Property.ofValue(neo4jContainer.getBoltUrl()))
            .username(Property.ofValue("neo4j"))
            .password(Property.ofValue(neo4jContainer.getAdminPassword()))
            .query(Property.ofValue(cypher))
            .storeType(Property.ofValue(storeType))
            .build();
    }

    private Trigger createTrigger(String cypher) {
        return createTrigger(cypher, StoreType.FETCH);
    }

    private Optional<Execution> evaluateTrigger(Trigger trigger) throws Exception {
        var mocked = TestsUtils.mockTrigger(runContextFactory, trigger);
        return trigger.evaluate(mocked.getKey(), mocked.getValue());
    }

    @Test
    void triggersWhenRowsExist() throws Exception {
        Trigger trigger = createTrigger(
            "MERGE (p:TriggerTestPerson {name: 'Alice'}) " +
                "RETURN p {.name} AS person"
        );

        Optional<Execution> result = evaluateTrigger(trigger);

        assertTrue(result.isPresent(), "Expected an execution when query returns rows");
    }

    @Test
    void doesNotTriggerWhenNoRowsExist() throws Exception {
        Trigger trigger = createTrigger(
            "MATCH (p:TriggerTestPerson {name: 'this-person-does-not-exist'}) " +
                "RETURN p.name AS name"
        );

        Optional<Execution> result = evaluateTrigger(trigger);

        assertFalse(result.isPresent(), "Expected no execution when query returns no rows");
    }

    @Test
    void deletesStoredResultWhenNoRowsExist() throws Exception {
        Trigger trigger = createTrigger(
            "MATCH (p:TriggerTestPerson {name: 'this-person-does-not-exist'}) " +
                "RETURN p.name AS name",
            StoreType.STORE
        );

        var mocked = TestsUtils.mockTrigger(runContextFactory, trigger);
        var runContext = mocked.getKey().getRunContext();

        var uri = runContext.storage().putFile(
            new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8)),
            "trigger-test.ion"
        );

        assertNotNull(runContext.storage().getAttributes(uri));

        Query.Output output = Query.Output.builder()
            .uri(uri)
            .size(0L)
            .build();

        Query query = mock(Query.class);
        doReturn(output).when(query).run(any(RunContext.class));

        Trigger testTrigger = org.mockito.Mockito.spy(trigger);
        doReturn(query).when(testTrigger).createQuery();

        Optional<Execution> result = testTrigger.evaluate(
            mocked.getKey(),
            mocked.getValue()
        );

        assertFalse(
            result.isPresent(),
            "Expected no execution when query returns no rows"
        );

        assertFalse(
            runContext.storage().deleteFile(uri),
            "Expected stored result file to have been deleted"
        );
    }
}
