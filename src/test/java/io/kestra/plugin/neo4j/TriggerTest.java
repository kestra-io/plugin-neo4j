package io.kestra.plugin.neo4j;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

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
        var trigger = createTrigger(
            "MERGE (p:TriggerTestPerson {name: 'Alice'}) " +
                "RETURN p {.name} AS person"
        );

        var result = evaluateTrigger(trigger);

        assertTrue(result.isPresent(), "Expected an execution when query returns rows");
    }

    @Test
    void doesNotTriggerWhenNoRowsExist() throws Exception {
        var trigger = createTrigger(
            "MATCH (p:TriggerTestPerson {name: 'this-person-does-not-exist'}) " +
                "RETURN p.name AS name"
        );

        var result = evaluateTrigger(trigger);

        assertFalse(result.isPresent(), "Expected no execution when query returns no rows");
    }

    @Test
    void deletesStoredResultWhenNoRowsExist() throws Exception {
        var trigger = createTrigger(
            "MATCH (p:TriggerTestPerson {name: 'this-person-does-not-exist'}) " +
                "RETURN p.name AS name",
            StoreType.STORE
        );

        var mocked = TestsUtils.mockTrigger(runContextFactory, trigger);
        var runContext = mocked.getKey().getRunContext();

        var storedUri = new AtomicReference<URI>();
        var testTrigger = spy(trigger);
        var query = spy(trigger.createQuery());
        doAnswer(invocation ->
        {
            var output = (Query.Output) invocation.callRealMethod();
            storedUri.set(output.getUri());
            assertNotNull(runContext.storage().getAttributes(output.getUri()));
            return output;
        }).when(query).run(any(RunContext.class));
        doReturn(query).when(testTrigger).createQuery();

        var result = testTrigger.evaluate(
            mocked.getKey(),
            mocked.getValue()
        );

        assertFalse(
            result.isPresent(),
            "Expected no execution when query returns no rows"
        );

        assertNotNull(storedUri.get(), "Expected STORE query to create a result file");
        assertThrows(
            FileNotFoundException.class,
            () -> runContext.storage().getAttributes(storedUri.get()),
            "Expected stored result file to have been deleted"
        );
    }

    @Test
    void storesResultsWhenRowsExist() throws Exception {
        var trigger = createTrigger("RETURN {name: 'Alice'} AS person", StoreType.STORE);
        var mocked = TestsUtils.mockTrigger(runContextFactory, trigger);
        var runContext = mocked.getKey().getRunContext();
        var storedOutput = new AtomicReference<Query.Output>();
        var query = spy(trigger.createQuery());
        doAnswer(invocation ->
        {
            var output = (Query.Output) invocation.callRealMethod();
            storedOutput.set(output);
            return output;
        }).when(query).run(any(RunContext.class));
        var testTrigger = spy(trigger);
        doReturn(query).when(testTrigger).createQuery();

        var result = testTrigger.evaluate(mocked.getKey(), mocked.getValue());

        assertTrue(result.isPresent(), "Expected STORE mode to create a trigger execution");
        assertNotNull(storedOutput.get().getUri(), "Expected STORE mode to create a result file");
        assertTrue(storedOutput.get().getSize() > 0, "Expected STORE mode to report stored rows");
        assertNotNull(runContext.storage().getAttributes(storedOutput.get().getUri()));
        runContext.storage().deleteFile(storedOutput.get().getUri());
    }

    @Test
    void rejectsNoneStoreType() throws Exception {
        var trigger = createTrigger("RETURN 1 AS value", StoreType.NONE);
        var mocked = TestsUtils.mockTrigger(runContextFactory, trigger);

        var exception = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(mocked.getKey(), mocked.getValue())
        );

        assertTrue(exception.getMessage().contains("NONE is not supported"));
    }
}
