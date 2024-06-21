package io.kestra.plugin.neo4j;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BatchTest {
    @Inject
    private RunContextFactory runContextFactory;
    @Inject
    private StorageInterface storageInterface;

    static String query() {
        return "UNWIND $props AS properties" + "\n" +
            "CREATE (n:Person)" + "\n" +
            "SET n = properties" + "\n" +
            "RETURN n";
    }

    @Container
    private final static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"));

    @Test
    void batchCreate() throws Exception {
        Batch batch = Batch.builder()
            .id(IdUtils.create())
            .type(Batch.class.getName())
            .query(query())
            .url(neo4jContainer.getBoltUrl())
            .username("neo4j")
            .password(neo4jContainer.getAdminPassword())
            .from(createTestFile().toString())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, batch, ImmutableMap.of());
        Batch.Output run = batch.run(runContext);

        assertThat(run.getUpdatedCount(), is(25000));
        assertThat(run.getRowCount().intValue(), is(25));
    }

    URI createTestFile() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);
        for (int i = 0; i < 25000; i++) {
            Map<String, Object> n1 = new HashMap<>();
            n1.put("name", UUID.randomUUID().toString());
            n1.put("position", UUID.randomUUID().toString());
            FileSerde.write(output, n1);
        }
        return storageInterface.put(null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}
