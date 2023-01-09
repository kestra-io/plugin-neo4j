package io.kestra.plugin.neo4j;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test will only test the main task, this allow you to send any input
 * parameters to your task and test the returning behaviour easily.
 */
@MicronautTest
@Testcontainers
class QueryTaskTest {
    @Inject
    private RunContextFactory runContextFactory;
    @Inject
    private StorageInterface storageInterface;

    static String query() {
        return "MATCH (p:Person) \n" +
                "RETURN p";
        /*return "SELECT \n" +
                "  \"hello\" as string,\n" +
                "  CAST(NULL AS INT) AS `nullable`,\n" +
                "  TRUE AS `bool`,\n" +
                "  1 as int,\n" +
                "  1.25 AS float,\n" +
                "  DATE(\"2008-12-25\") AS date,\n" +
                "  DATETIME \"2008-12-25 15:30:00.123456\" AS datetime,\n" +
                "  TIME(DATETIME \"2008-12-25 15:30:00.123456\") AS time,\n" +
                "  TIMESTAMP(\"2008-12-25 15:30:00.123456\") AS timestamp,\n" +
                "  ST_GEOGPOINT(50.6833, 2.9) AS geopoint,\n" +
                "  ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS `array`,\n" +
                "  STRUCT(NULL as v, 4 AS x, 0 AS y, ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS z) AS `struct`";*/
    }

    @Container
    private final static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"));

    @BeforeEach
    void initDatabase() {
        // Retrieve the Bolt URL from the container
        String boltUrl = neo4jContainer.getBoltUrl();
        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword())); Session session = driver.session()) {
            session.run("CREATE (p:Person {" +
                    "name: 'aDeveloper', " +
                    "friends: ['otherDevelopers', 'PO', 'otherQas']" +
                    "})");
            session.run("CREATE (p:Person {" +
                    "name: 'aQa', " +
                    "friends: ['otherQas', 'otherDevelopers']" +
                    "})");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetch() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of(
                "query", query(),
                "url", neo4jContainer.getBoltUrl(),
                "username", "neo4j",
                "password", neo4jContainer.getAdminPassword(),
                "flow", ImmutableMap.of("id", 1, "namespace", "io.kestra.tests"),
                "execution", ImmutableMap.of("id", 1),
                "taskrun", ImmutableMap.of("id", 1)
        ));

        QueryTask queryTask = QueryTask.builder()
                .query("{{query}}")
                .url("{{url}}")
                .username("{{username}}")
                .password("{{password}}")
                .fetch(true)
                .store(false)
                .build();

        QueryTask.Output run = queryTask.run(runContext);

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(2));

        assertThat(rows.get(0).get("name"), is("aDeveloper"));
        assertThat((List<String>) rows.get(0).get("friends"), containsInAnyOrder("otherDevelopers", "PO", "otherQas"));
        assertThat(rows.get(1).get("name"), is("aQa"));
        assertThat((List<String>) rows.get(1).get("friends"), containsInAnyOrder("otherQas", "otherDevelopers"));

        /*assertThat(rows.get(0).get("int"), is(1L));
        assertThat(rows.get(0).get("float"), is(1.25D));
        assertThat(rows.get(0).get("date"), is(LocalDate.parse("2008-12-25")));
        assertThat(rows.get(0).get("time"), is(LocalTime.parse("15:30:00.123456")));
        assertThat(rows.get(0).get("timestamp"), is(Instant.parse("2008-12-25T15:30:00.123Z")));
        assertThat((List<Double>) rows.get(0).get("geopoint"), containsInAnyOrder(50.6833, 2.9));
        assertThat((List<Long>) rows.get(0).get("array"), containsInAnyOrder(1L, 2L, 3L));
        assertThat(((Map<String, Object>) rows.get(0).get("struct")).get("v"), is(nullValue()));
        assertThat(((Map<String, Object>) rows.get(0).get("struct")).get("x"), is(4L));
        assertThat(((Map<String, Object>) rows.get(0).get("struct")).get("y"), is(0L));
        assertThat((List<Long>) ((Map<String, Object>) rows.get(0).get("struct")).get("z"), containsInAnyOrder(1L, 2L, 3L));*/
    }

    /*@Test
    void store() throws Exception {
        Query task = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql(query() + "\n UNION ALL \n " + query())
                .store(true)
                .build();

        Query.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(
                CharStreams.toString(new InputStreamReader(storageInterface.get(run.getUri()))),
                is(StringUtils.repeat(
                        "{string:\"hello\",nullable:null,bool:true,int:1,float:1.25e0,date:2008-12-25,datetime:2008-12-25T15:30:00.123Z,time:LocalTime::\"15:30:00.123456\",timestamp:2008-12-25T15:30:00.123Z,geopoint:[50.6833e0,2.9e0],array:[1,2,3],struct:{v:null,x:4,y:0,z:[1,2,3]}}\n",
                        2
                ))
        );
    }

    @Test
    void fetchLongPage() throws Exception {
        Query task = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql("SELECT repository_forks FROM `bigquery-public-data.samples.github_timeline` LIMIT 100000")
                .fetch(true)
                .build();

        Query.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        List<Map<String, Object>> rows = run.getRows();
        assertThat(rows.size(), is(100000));
    }

    @Test
    void destination() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();
        Query task = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql("{% for input in inputs.loop %}" +
                        "SELECT" +
                        "  \"{{execution.id}}\" as execution_id," +
                        "  TIMESTAMP \"{{execution.startDate | date(\"yyyy-MM-dd HH:mm:ss.SSSSSS\")}}\" as execution_date," +
                        "  {{ input }} as counter" +
                        "{{ loop.last  == false ? '\nUNION ALL\n' : '\n' }}" +
                        "{% endfor %}"
                )
                .destinationTable(project + "." + dataset + "." + friendlyId)
                .timePartitioningField("execution_date")
                .clusteringFields(Arrays.asList("execution_id", "counter"))
                .schemaUpdateOptions(Collections.singletonList(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION))
                .writeDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
                "loop", ContiguousSet.create(Range.closed(1, 25), DiscreteDomain.integers())
        ));

        Query.Output run = task.run(runContext);
        assertThat(run.getJobId(), is(notNullValue()));
        assertThat(run.getDestinationTable().getProject(), is(project));
        assertThat(run.getDestinationTable().getDataset(), is(dataset));
        assertThat(run.getDestinationTable().getTable(), is(friendlyId));
    }

    @Test
    void error() {
        Query task = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql("SELECT * from `{{execution.id}}`")
                .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        FailsafeException e = assertThrows(FailsafeException.class, () -> {
            task.run(runContext);
        });

        assertThat(e.getCause().getMessage(), containsString("must be qualified with a dataset"));
    }

    @Test
    void script() throws Exception {
        Query task = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql("{% for input in inputs.loop %}" +
                        "SELECT" +
                        "  \"{{execution.id}}\" as execution_id," +
                        "  TIMESTAMP \"{{execution.startDate | date(\"yyyy-MM-dd HH:mm:ss.SSSSSS\") }}\" as execution_date," +
                        "  {{input}} as counter;" +
                        "{% endfor %}"
                )
                .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
                "loop", ContiguousSet.create(Range.closed(1, 2), DiscreteDomain.integers())
        ));

        Query.Output run = task.run(runContext);
        assertThat(run.getJobId(), is(notNullValue()));
    }

    @Test
    void retry() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();
        String table = project + "." + dataset + "." + FriendlyId.createFriendlyId();

        List<Callable<Query.Output>> tasks = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            Query task = Query.builder()
                    .id(QueryTest.class.getSimpleName())
                    .type(Query.class.getName())
                    .sql("SELECT \"" + i + "\" as value")
                    .destinationTable(table)
                    .createDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                    .writeDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                    .build();

            RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

            tasks.add(() -> task.run(runContext));
        }

        List<Future<Query.Output>> futures = executorService.invokeAll(tasks);
        executorService.shutdown();

        List<Query.Output> results = futures
                .stream()
                .map(outputFuture -> {
                    try {
                        return outputFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Failed on ", e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        assertThat(results.size(), is(tasks.size()));
    }

    @Test
    void scriptError() {
        Query task = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql("{% for input in  inputs.loop %}" +
                        "SELECT * from `{{execution.id}}`;" +
                        "{% endfor %}"
                )
                .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of(
                "loop", ContiguousSet.create(Range.closed(1, 2), DiscreteDomain.integers())
        ));

        FailsafeException e = assertThrows(FailsafeException.class, () -> {
            task.run(runContext);
        });

        assertThat(e.getMessage(), containsString("must be qualified with a dataset"));
    }

    @Test
    void scriptMultipleNoOutput() throws Exception {
        Query task = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql("DROP TABLE IF EXISTS `" + project + "." + this.dataset + ".not`;" +
                        "DROP TABLE IF EXISTS `" + project + "." + this.dataset + ".exist`;"
                )
                .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        Query.Output run = task.run(runContext);

        assertThat(run.getDestinationTable(), nullValue());
        assertThat(run.getJobId(), notNullValue());
    }

    @Test
    @Disabled
    void concurrency() throws Exception {
        String table = this.dataset + "." + FriendlyId.createFriendlyId();

        Query create = Query.builder()
                .id(QueryTest.class.getSimpleName())
                .type(Query.class.getName())
                .sql("CREATE TABLE " + table + " AS SELECT 1 AS number")
                .build();

        create.run(TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of()));

        ExecutorService executorService = Executors.newFixedThreadPool(250);

        final int COUNT = 1000;

        CountDownLatch countDownLatch = new CountDownLatch(COUNT);

        Query task = Query.builder()
                .id("test")
                .type(Query.class.getName())
                .sql("SELECT * FROM " + table + ";")
                .fetchOne(true)
                .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        for (int i = 0; i < COUNT; i++) {
            executorService.execute(throwRunnable(() -> {
                Query.Output result = task.run(runContext);
                assertThat(result.getRow().get("number"), is(1L));

                countDownLatch.countDown();
            }));
        }

        countDownLatch.await();
    }*/
}