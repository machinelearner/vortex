package org.vortex.domain;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.vortex.Settings;
import org.vortex.help.Callable1;
import org.vortex.help.Maps;
import org.vortex.impl.target.FileTarget;
import org.vortex.impl.target.MongoTarget;
import org.vortex.query.CreateQuery;
import org.vortex.query.ListQuery;
import org.vortex.query.UpdateQuery;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static name.mlnkrishnan.shouldJ.ShouldJ.it;

public class FlowTest {

    public static final String TEST_DB_NAME = "vortex_test";
    private static final int PAGE_SIZE = 4;
    private Settings defaults;

    CountDownLatch countDown;
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8090);
    private MongoTarget mongoTarget;
    private FileTarget fileTarget;
    private String[] testCollections;
    private DB testDb;
    private Map<String, Object> dev1;
    private Map<String, Object> dev2;
    private Map<String, Object> dev3;
    private Map<String, Object> dev4;
    private Map<String, Object> dev5;
    private Map<String, Object> dev6;
    private Map<String, Object> dev7;
    private Map<String, Object> dev8;
    private Map<String, Object> dev9;
    private Map<String, Object> dev10;
    private Map<String, List<Map<String, Object>>> pagedDevs;


    @Before
    public void setUp() throws Exception {
        defaults = Settings.overrideDefaults(new Settings() {{
            put("mongo.db.Url", "mongodb://localhost:27017");
            put("mongo.db.name", TEST_DB_NAME);
            put("http.notification.url", "http://localhost:8090/notify");
        }});

        countDown = new CountDownLatch(1);
        stubFor(post(urlEqualTo("/notify"))
                .willReturn(aResponse().withStatus(200)));
        testCollections = new String[]{"vortex.source.sample", "vortex.dest.sample"};
        mongoTarget = new MongoTarget(defaults);
        fileTarget = new FileTarget(Settings.defaults());
        testDb = new MongoClient(new MongoClientURI(defaults.dbUrl("mongo"))).getDB(TEST_DB_NAME);
        dev1 = Maps.<String, Object>map("id", 1, "tenantId", "dev");
        dev2 = Maps.<String, Object>map("id", 2, "tenantId", "dev");
        dev3 = Maps.<String, Object>map("id", 3, "tenantId", "dev");
        dev4 = Maps.<String, Object>map("id", 4, "tenantId", "dev");
        dev5 = Maps.<String, Object>map("id", 5, "tenantId", "dev");
        dev6 = Maps.<String, Object>map("id", 6, "tenantId", "dev");
        dev7 = Maps.<String, Object>map("id", 7, "tenantId", "dev");
        dev8 = Maps.<String, Object>map("id", 8, "tenantId", "dev");
        dev9 = Maps.<String, Object>map("id", 9, "tenantId", "dev");
        dev10 = Maps.<String, Object>map("id", 10, "tenantId", "dev");
        pagedDevs = Maps.map("page1", Arrays.asList(dev1, dev2, dev3, dev4), "page2", Arrays.asList(dev5, dev6, dev7, dev8), "page3", Arrays.asList(dev9, dev10));

    }

    @After
    public void tearDown() {
        dropTestCollections();
        deleteTempFile();
    }


    @Test
    public void shouldExecuteFlow_MongoExample_NewFieldAddition() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.dest.sample")
                .where("tenantId", "dev")
                .sortBy("id", "1")
                .pageSize(PAGE_SIZE)
                .pageStart("id", 0);
        Callable1<QueryResult, VQuery> InplaceUpdateWithNewField = new Callable1<QueryResult, VQuery>() {
            @Override
            public VQuery call(QueryResult queryResult) {
                Map<String, Object> result = queryResult.result();
                result.put("newField", result.get("id"));
                return new UpdateQuery()
                        .into(queryResult.from())
                        .where("_id", result.get("_id"))
                        .object(result)
                        .upsert(true);
            }
        };
        Flow flow = new FlowBuilder()
                .withSource(mongoTarget, listExpertiseFirstPageOfDevTenant)
                .withSink(mongoTarget)
                .withTransformer(InplaceUpdateWithNewField)
                .build();


        Result flowExecution = flow.execute().call();


        it(flowExecution.isSuccess()).shouldBeTrue();
        Result listAll = mongoTarget.list(new ListQuery()
                .from("vortex.dest.sample")
                .sortBy("_id", "1")).call();
        List<Map> results = (List<Map>) listAll.result().get("result");
        it(results).shouldBeOfSize(11);
        for (int i = 1; i <= 10; i++) {
            int index = i - 1;
            it(results.get(index).get("id")).shouldBe(i);
            it(results.get(index).get("tenantId")).shouldBe("dev");
            it(results.get(index).get("newField")).shouldBe(i);
        }

    }

    @Test
    public void shouldExecuteFlow_AndGiveResultWithMetadata() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.dest.sample")
                .where("tenantId", "dev")
                .sortBy("id", "1")
                .pageSize(PAGE_SIZE)
                .pageStart("id", 0);
        Callable1<QueryResult, VQuery> InplaceUpdateWithNewField = new Callable1<QueryResult, VQuery>() {
            @Override
            public VQuery call(QueryResult queryResult) {
                Map<String, Object> result = queryResult.result();
                result.put("newField", result.get("id"));
                return new UpdateQuery()
                        .into(queryResult.from())
                        .where("_id", result.get("_id"))
                        .object(result)
                        .upsert(true);
            }
        };
        Flow flow = new FlowBuilder()
                .withSource(mongoTarget, listExpertiseFirstPageOfDevTenant)
                .withSink(mongoTarget)
                .withTransformer(InplaceUpdateWithNewField)
                .build()
                .withMetadata(Maps.<String, Object>map("_id", "blah"));


        Result flowExecution = flow.execute().call();


        it(flowExecution.isSuccess()).shouldBeTrue();
        it(flowExecution.result().get("_id")).shouldBe("blah");
        Result listAll = mongoTarget.list(new ListQuery()
                .from("vortex.dest.sample")
                .sortBy("_id", "1")).call();
        List<Map> results = (List<Map>) listAll.result().get("result");
        it(results).shouldBeOfSize(11);
        for (int i = 1; i <= 10; i++) {
            int index = i - 1;
            it(results.get(index).get("id")).shouldBe(i);
            it(results.get(index).get("tenantId")).shouldBe("dev");
            it(results.get(index).get("newField")).shouldBe(i);
        }

    }


    @Test
    public void shouldExecuteFlow_FileExample() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.dest.sample")
                .where("tenantId", "dev")
                .sortBy("id", "1")
                .pageSize(PAGE_SIZE)
                .pageStart("id", 0);

        final Callable1<QueryResult, VQuery> filterReport = new Callable1<QueryResult, VQuery>() {
            @Override
            public VQuery call(QueryResult queryResult) throws Exception {
                Map<String, Object> result = queryResult.result();
                return new UpdateQuery()
                        .into("tmp.csv")
                        .object(Maps.map(result.get("id").toString(), result.get("tenantId")));
            }
        };
        Flow flow = new FlowBuilder()
                .withSource(mongoTarget, listExpertiseFirstPageOfDevTenant)
                .withSink(fileTarget)
                .withTransformer(filterReport)
                .build();

        Result flowExecution = flow.execute().call();

        it(flowExecution.isSuccess()).shouldBeTrue();
        List<String> lines = Files.readAllLines(new File("tmp.csv").toPath());
        it(lines).shouldBeOfSize(10);
        for (int i = 1; i <= 10; i++) {
            int index = i - 1;
            it(lines.get(index)).shouldBe(String.valueOf(i) + ",dev");
        }

    }

    @Test
    public void shouldExecuteFlow_MongoExample_ModifyExistingField() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.dest.sample")
                .where("tenantId", "dev")
                .sortBy("id", "1")
                .pageSize(PAGE_SIZE)
                .pageStart("id", 0);
        Callable1<QueryResult, VQuery> InplaceUpdateWithNewField = new Callable1<QueryResult, VQuery>() {
            @Override
            public VQuery call(QueryResult queryResult) throws Exception {
                Map<String, Object> result = queryResult.result();
                result.put("tenantId", "afterModification"); //change Existing field
                return new UpdateQuery()
                        .into(queryResult.from())
                        .where("_id", result.get("_id"))
                        .object(result)
                        .upsert(true);
            }
        };
        Flow flow = new FlowBuilder()
                .withSource(mongoTarget, listExpertiseFirstPageOfDevTenant)
                .withSink(mongoTarget)
                .withTransformer(InplaceUpdateWithNewField)
                .build();


        Result flowExecution = flow.execute().call();


        it(flowExecution.isSuccess()).shouldBeTrue();
        Result listAll = mongoTarget.list(new ListQuery()
                .from("vortex.dest.sample")
                .sortBy("_id", "1")).call();
        List<Map> results = (List<Map>) listAll.result().get("result");
        it(results).shouldBeOfSize(11);
        for (int i = 1; i <= 10; i++) {
            int index = i - 1;
            it(results.get(index).get("id")).shouldBe(i);
            it(results.get(index).get("tenantId")).shouldBe("afterModification");
        }
    }

    @Test
    public void shouldExecuteFlow_MongoExample_WriteToDifferentCollection() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.dest.sample")
                .where("tenantId", "dev")
                .sortBy("id", "1")
                .pageSize(PAGE_SIZE)
                .pageStart("id", 0);
        Callable1<QueryResult, VQuery> InplaceUpdateWithNewField = new Callable1<QueryResult, VQuery>() {
            @Override
            public VQuery call(QueryResult queryResult) throws Exception {
                Map<String, Object> result = queryResult.result();
                result.put("newField", "hello"); //change Existing field
                return new UpdateQuery()
                        .into("vortex.sample.refactored")
                        .where("_id", result.get("_id"))
                        .object(result)
                        .upsert(true);
            }
        };
        Flow flow = new FlowBuilder()
                .withSource(mongoTarget, listExpertiseFirstPageOfDevTenant)
                .withSink(mongoTarget)
                .withTransformer(InplaceUpdateWithNewField)
                .build();


        Result flowExecution = flow.execute().call();


        it(flowExecution.isSuccess()).shouldBeTrue();
        Result listAll = mongoTarget.list(new ListQuery()
                .from("vortex.sample.refactored")
                .sortBy("_id", "1")).call();
        List<Map> results = (List<Map>) listAll.result().get("result");
        it(results).shouldBeOfSize(10);
        for (int i = 1; i <= 10; i++) {
            int index = i - 1;
            it(results.get(index).get("id")).shouldBe(i);
            it(results.get(index).get("tenantId")).shouldBe("dev");
            it(results.get(index).get("newField")).shouldBe("hello");
        }
        dropCollection("vortex.sample.refactored");

    }

    @Test
    public void shouldExecuteFlow_MongoExample_WriteMultipleObjects() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.dest.sample")
                .where("tenantId", "dev")
                .sortBy("id", "1")
                .pageSize(PAGE_SIZE)
                .pageStart("id", 0);
        Function<QueryResult, List<VQuery>> MultipleObjectWrite = new Function<QueryResult, List<VQuery>>() {
            @Override
            public List<VQuery> apply(QueryResult queryResult) {
                final Map<String, Object> result = queryResult.result();
                List<VQuery> taskQueries = Stream.of(0, 1)
                        .map((Function<Integer, VQuery>) integer -> {
                            Map<String, Object> muxed = Maps.map("id", result.get("id"), "muxed", integer.intValue(), "tenantId", result.get("tenantId"), "new", result.get("id"));
                            return new CreateQuery()
                                    .into("vortex.sample.refactored")
                                    .object(muxed);
                        }).collect(Collectors.toList());
                return taskQueries;
            }
        };
        Flow flow = new FlowBuilder()
                .withSource(mongoTarget, listExpertiseFirstPageOfDevTenant)
                .withSink(mongoTarget)
                .withTransformer(MultipleObjectWrite)
                .build();


        Result flowExecution = flow.execute().call();


        it(flowExecution.isSuccess()).shouldBeTrue();
        Result listAll = mongoTarget.list(new ListQuery()
                .from("vortex.sample.refactored")
                .sortBy("_id", "1")).call();
        List<Map> results = (List<Map>) listAll.result().get("result");
        it(results).shouldBeOfSize(20);
        int id = 1;
        for (int i = 0; i < 20; i += 2) {
            it(results.get(i).get("id")).shouldBe(id);
            it(results.get(i).get("tenantId")).shouldBe("dev");
            it(results.get(i).get("muxed")).shouldBe(0);

            it(results.get(i + 1).get("id")).shouldBe(id);
            it(results.get(i + 1).get("tenantId")).shouldBe("dev");
            it(results.get(i + 1).get("muxed")).shouldBe(1);
            id += 1;
        }
        dropCollection("vortex.sample.refactored");

    }

    /**
     * Not advisable to write to different collections as this is conceptually a separate flow with the same source
     * <p>
     * But if the transformation is same - same copy of object in two locations, its perhaps ok to use it this way
     */
    @Test
    public void shouldExecuteFlow_MongoExample_WriteMultipleMultipleCollections() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.dest.sample")
                .where("tenantId", "dev")
                .sortBy("id", "1")
                .pageSize(PAGE_SIZE)
                .pageStart("id", 0);
        Function<QueryResult, List<VQuery>> MultipleCollectionWrite = new Function<QueryResult, List<VQuery>>() {
            @Override
            public List<VQuery> apply(QueryResult queryResult) {
                final Map<String, Object> result = queryResult.result();
                List<VQuery> taskQueries = Stream.of(0, 1)
                        .map(new Function<Integer, VQuery>() {
                            @Override
                            public VQuery apply(Integer integer) {
                                String collectionName = "vortex.sample.refactored." + integer.toString();
                                return new CreateQuery()
                                        .into(collectionName)
                                        .object(result);
                            }
                        }).collect(Collectors.toList());
                return taskQueries;
            }
        };
        Flow flow = new FlowBuilder()
                .withSource(mongoTarget, listExpertiseFirstPageOfDevTenant)
                .withSink(mongoTarget)
                .withTransformer(MultipleCollectionWrite)
                .build();


        Result flowExecution = flow.execute().call();


        it(flowExecution.isSuccess()).shouldBeTrue();
        Stream.of(0, 1).map(new Function<Number, Object>() {
            @Override
            public Object apply(Number number) {
                Result listAll = null;
                try {
                    listAll = mongoTarget.list(new ListQuery()
                            .from("vortex.sample.refactored." + number.toString())
                            .sortBy("_id", "1")).call();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                List<Map> results = (List<Map>) listAll.result().get("result");
                it(results).shouldBeOfSize(10);
                for (int i = 1; i <= 10; i++) {
                    int index = i - 1;
                    it(results.get(index).get("id")).shouldBe(i);
                    it(results.get(index).get("tenantId")).shouldBe("dev");
                }
                return null;
            }
        }).collect(Collectors.toList());
        dropCollection("vortex.sample.refactored");
        dropCollection("vortex.sample.refactored.0");
        dropCollection("vortex.sample.refactored.1");

    }

    private void insertSeedInTestCollections() {
        for (String aCollection : testCollections) {
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev1));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev2));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev3));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev4));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev5));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev6));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev7));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev8));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev9));
            testDb.getCollection(aCollection).insert(new BasicDBObject(dev10));
            testDb.getCollection(aCollection).insert(new BasicDBObject(Maps.map("id", 3, "tenantId", "t1")));
        }
    }

    private void dropTestCollections() {
        for (String testCollection : testCollections) {
            dropCollection(testCollection);
        }
        dropCollection("vortex.sample.refactored");
    }

    private void dropCollection(String testCollection) {
        testDb.getCollection(testCollection).drop();
    }

    private void deleteTempFile() {
        try {
            Files.deleteIfExists(new File("tmp.csv").toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}