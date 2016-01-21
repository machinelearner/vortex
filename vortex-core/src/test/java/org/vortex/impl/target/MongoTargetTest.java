package org.vortex.impl.target;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.help.Maps;
import org.vortex.help.Pair;
import org.vortex.query.BulkDeleteTaskQuery;
import org.vortex.query.CreateQuery;
import org.vortex.query.ListQuery;
import org.vortex.query.UpdateQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static name.mlnkrishnan.shouldJ.ShouldJ.it;
import static org.vortex.help.Pair.pair;

public class MongoTargetTest {
    public static final String TEST_DB_NAME = "vortex_test";
    public static final int PAGE_SIZE = 4;
    private String[] testCollections;
    private MongoTarget mongoTarget;
    private Settings defaults;
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
    private Map<String, Object> t1_1;
    private Map<String, List<Map<String, Object>>> pagedDevs;
    @org.junit.Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        defaults = Settings.overrideDefaults(new Settings(){{
            put("mongo.db.name", TEST_DB_NAME);
        }});
        testCollections = new String[]{"vortex.baseSample", "vortex.sample"};
        mongoTarget = new MongoTarget(defaults);
        testDb = mongoTarget.initialize(defaults).getDB(TEST_DB_NAME);
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
        pagedDevs = Maps.map("page1", asList(dev1, dev2, dev3, dev4), "page2", asList(dev5, dev6, dev7, dev8), "page3", asList(dev9, dev10));
        t1_1 = Maps.<String, Object>map("id", 3, "tenantId", "t1");

        dropTestCollections();
    }

    private void dropTestCollections() {
        for (String testCollection : testCollections) {
            testDb.getCollection(testCollection).drop();
        }
    }

    @After
    public void tearDown() {
        dropTestCollections();
    }

    @Test
    public void shouldQueryFromCollection_CheckingForDbNameFromSettings() throws Exception {
        Settings pickDBNameSettings = Settings.overrideDefaults(new Settings() {{
            put("mongo.db.name", TEST_DB_NAME);
        }});
        MongoTarget mongoTarget = new MongoTarget(pickDBNameSettings);
        insertSeedInTestCollections();
        ListQuery listExpertiseDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC);
        Result result = mongoTarget.list(listExpertiseDevTenant).call();
        it(result.isSuccess()).shouldBeTrue();
        List results = (List) result.result().get("result");
        it(results).shouldBeOfSize(10);
        verifyPage(results.subList(0, 4), pagedDevs.get("page1"));
        verifyPage(results.subList(4, 8), pagedDevs.get("page2"));
        verifyPage(results.subList(8, 10), pagedDevs.get("page3"));
    }


    @Test
    public void shouldQueryFromCollection_CheckingForFieldExistsFromSettings() throws Exception {
        Settings pickDBNameSettings = Settings.overrideDefaults(new Settings() {{
            put("mongo.db.name", TEST_DB_NAME);
        }});

        MongoTarget mongoTarget = new MongoTarget(pickDBNameSettings);
        insertSeedInTestCollections();
        ListQuery listExpertiseDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", new BasicDBObject("$exists", true))
                .sortBy("_id", ListQuery.SortOrder.ASC);
        Result result = mongoTarget.list(listExpertiseDevTenant).call();
        it(result.isSuccess()).shouldBeTrue();
        List results = (List) result.result().get("result");
        it(results).shouldBeOfSize(11);
    }

    @Test
    @Ignore("Half baked impl for bulk delete; Soon will be revisited")
    public void shouldDeleteGivenACollectionAndConditions() throws Exception {
        insertSeedInTestCollections();
        BulkDeleteTaskQuery deleteAllRecordsOfATenantAndApp = new BulkDeleteTaskQuery()
                .condition("tenantId", "dev");

        Result deleteMongoResult = mongoTarget.delete(deleteAllRecordsOfATenantAndApp).call();

        it(deleteMongoResult.isSuccess()).shouldBeTrue();
        verifyRemainingInAllCollections();

    }

    @Test
    public void shouldListRecordsInACollection_PageSize4() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageSize(PAGE_SIZE);
        Result result = mongoTarget.list(listExpertiseFirstPageOfDevTenant).call();
        it(result.isSuccess()).shouldBeTrue();
        List results = (List) result.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, pagedDevs.get("page1"));
    }

    @Test
    public void shouldListRecordsInACollection_NoWhereClause() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .sortBy("_id", ListQuery.SortOrder.ASC);
        Result result = mongoTarget.list(listExpertiseFirstPageOfDevTenant).call();
        it(result.isSuccess()).shouldBeTrue();
        List results = (List) result.result().get("result");
        it(results).shouldBeOfSize(11);
        verifyPage(results.subList(0, 4), pagedDevs.get("page1"));
        verifyPage(results.subList(4, 8), pagedDevs.get("page2"));
        verifyPage(results.subList(8, 10), pagedDevs.get("page3"));
        verifyPage(results.subList(10, 11), Arrays.asList(t1_1));
    }

    @Test
    public void shouldListRecordsInACollection_PageSize4_PageNumber2() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseSecondPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageStart("id", 4)
                .pageSize(PAGE_SIZE);
        Result result = mongoTarget.list(listExpertiseSecondPageOfDevTenant).call();
        it(result.isSuccess()).shouldBeTrue();
        List results = (List) result.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, pagedDevs.get("page2"));
    }

    /**
     * Showcases how to query in a paginated fashion using the page start parameter in {@link ListQuery}
     */
    @Test
    public void shouldListRecordsInACollection_PageSize4_PageNumber1_2_3() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageSize(PAGE_SIZE);
        ListQuery listExpertiseSecondPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageStart("id", PAGE_SIZE)
                .pageSize(PAGE_SIZE);
        ListQuery listExpertiseThirdPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageStart("id", 2 * PAGE_SIZE)
                .pageSize(PAGE_SIZE);
        Result aPage = mongoTarget.list(listExpertiseFirstPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        List results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, pagedDevs.get("page1"));

        aPage = mongoTarget.list(listExpertiseSecondPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, pagedDevs.get("page2"));

        aPage = mongoTarget.list(listExpertiseThirdPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(2);
        verifyPage(results, pagedDevs.get("page3"));
    }

    /**
     * Showcases how to query in a paginated fashion using the page start parameter in {@link ListQuery}
     * without using the PageSize parameter to manipulate start but instead using the last of previous result
     */
    @Test
    public void shouldListRecordsInACollection_PageSize4_PageNumber1_2_3_UsingLastIdOfPreviousResult() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageSize(PAGE_SIZE);

        Result aPage = mongoTarget.list(listExpertiseFirstPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        List results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, pagedDevs.get("page1"));

        Object previousResultId = ((Map) results.get(PAGE_SIZE - 1)).get("_id");
        ListQuery listExpertiseSecondPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageStart("_id", previousResultId)
                .pageSize(PAGE_SIZE);

        aPage = mongoTarget.list(listExpertiseSecondPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, pagedDevs.get("page2"));

        ListQuery listExpertiseThirdPageOfDevTenant = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageStart("_id", ((Map) results.get(PAGE_SIZE - 1)).get("_id"))
                .pageSize(PAGE_SIZE);

        aPage = mongoTarget.list(listExpertiseThirdPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(2);
        verifyPage(results, pagedDevs.get("page3"));
    }

    @Test
    public void shouldCreateDocumentGivenCreateQuery() throws Exception {
        CreateQuery createTwer1 = new CreateQuery()
                .into("vortex.baseSample")
                .object(Maps.<String, Object>map("id", 1, "tenantId", "vortex"));
        mongoTarget.create(createTwer1).call();

        Result result = mongoTarget.list(new ListQuery()
                .from("vortex.baseSample")).call();
        List<Map> results = (List<Map>) result.result().get("result");
        it(results).shouldBeOfSize(1);
        it(results.get(0).get("id")).shouldBe(1);
        it(results.get(0).get("tenantId")).shouldBe("vortex");
    }

    @Test
    public void shouldUpdateDocumentGivenUpdateQuery() throws Exception {
        insertSeedInTestCollections();

        Result result = mongoTarget.list(new ListQuery()
                .from("vortex.sample")).call();
        List<Map> results = (List<Map>) result.result().get("result");
        it(results).shouldBeOfSize(11); //Current Number of records

        UpdateQuery updateDev1 = new UpdateQuery()
                .into("vortex.sample")
                .where("id", 1)
                .object(Maps.<String, Object>map(dev1, Pair.<String, Object>pair("anotherField", 1)))
                .upsert(true);

        mongoTarget.update(updateDev1).call();

        result = mongoTarget.list(new ListQuery()
                .from("vortex.sample")
                .sortBy("_id", ListQuery.SortOrder.ASC)).call();
        results = (List<Map>) result.result().get("result");
        it(results).shouldBeOfSize(11);
        it(results.get(0).get("id")).shouldBe(1);
        it(results.get(0).get("tenantId")).shouldBe("dev");
        it(results.get(0).get("anotherField")).shouldBe(1);
    }

    @Test
    public void shouldUpdateDocumentGivenUpdateQuery_PartialUpdate_SetOperator() throws Exception {
        insertSeedInTestCollections();

        Result result = mongoTarget.list(new ListQuery()
                .from("vortex.sample")).call();
        List<Map> results = (List<Map>) result.result().get("result");
        it(results).shouldBeOfSize(11); //Current Number of records

        UpdateQuery updateDev1 = new UpdateQuery()
                .into("vortex.sample")
                .where("id", 1)
                .object(Maps.<String, Object>map(pair("$set", Maps.map("tenantId", "changed", "nonExistent", "bla"))))
                .upsert(false);

        mongoTarget.update(updateDev1).call();

        result = mongoTarget.list(new ListQuery()
                .from("vortex.sample")
                .sortBy("_id", ListQuery.SortOrder.ASC)).call();
        results = (List<Map>) result.result().get("result");
        it(results).shouldBeOfSize(11);
        it(results.get(0).get("id")).shouldBe(1);
        it(results.get(0).get("tenantId")).shouldBe("changed");
        it(results.get(0).get("nonExistent")).shouldBe("bla");
    }

    @Test
    public void shouldCreateDocumentGivenUpdateQuery_UpsertTrue() throws Exception {
        UpdateQuery updateDev20 = new UpdateQuery()
                .into("vortex.sample")
                .where("id", 20)
                .object(Maps.<String, Object>map("id", 20, "tenantId", "dev"))
                .upsert(true);

        mongoTarget.update(updateDev20).call();

        Result result = mongoTarget.list(new ListQuery()
                .from("vortex.sample")
                .sortBy("_id", ListQuery.SortOrder.ASC)).call();
        List<Map> results = (List<Map>) result.result().get("result");
        it(results).shouldBeOfSize(1);
        it(results.get(0).get("id")).shouldBe(20);
        it(results.get(0).get("tenantId")).shouldBe("dev");
    }

    @Test
    public void shouldNotUpdateDocumentGivenUpdateQuery_MultipleMatches() throws Exception {
        insertSeedInTestCollections();

        Result result = mongoTarget.list(new ListQuery()
                .from("vortex.sample")).call();
        List<Map> results = (List<Map>) result.result().get("result");
        it(results).shouldBeOfSize(11); //Current Number of records

        UpdateQuery updateAllDev = new UpdateQuery()
                .into("vortex.sample")
                .where("tenantId", "dev")
                .object(Maps.<String, Object>map(dev1, Pair.<String, Object>pair("anotherField", 1)))
                .upsert(true);

        Result updateResult = mongoTarget.update(updateAllDev).call();
        it(updateResult.isSuccess()).shouldBeTrue();

        result = mongoTarget.list(new ListQuery()
                .from("vortex.sample")
                .sortBy("_id", ListQuery.SortOrder.ASC)).call();
        results = (List<Map>) result.result().get("result");
        it(results).shouldBeOfSize(11);
        it(results.get(0).get("id")).shouldBe(1);
        it(results.get(0).get("tenantId")).shouldBe("dev");
        it(results.get(0).get("anotherField")).shouldBe(1);
    }


    @Test
    public void shouldCountRecordsGivenCollectionsAndConditions() throws Exception {
        insertSeedInTestCollections();
        ListQuery listExpertiseQuery = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC);

        Result result = mongoTarget.count(listExpertiseQuery).call();
        Long count = (Long) ((Map) result.result().get("result")).get("count");
        it(count).shouldBe(10l);

        ListQuery listExpertiseQueryForT1 = new ListQuery()
                .from("vortex.sample")
                .where("tenantId", "t1")
                .sortBy("_id", ListQuery.SortOrder.ASC);

        result = mongoTarget.count(listExpertiseQueryForT1).call();
        count = (Long) ((Map) result.result().get("result")).get("count");
        it(count).shouldBe(1l);
    }

    private void verifyPage(List<Map> results, final List<Map<String, Object>> aPage) {
        it(results).shouldBeOfSize(aPage.size());
        results.stream().map((Function<Map, Map<String, Object>>) dbObject -> {
            Predicate<String> notPrimaryKey = other -> !other.equals("_id");
            Function<String, Pair<String, Object>> keyToValue = key -> pair(key, dbObject.get(key));
            return Maps.map(dbObject.keySet().stream()
                    .filter(notPrimaryKey)
                    .map(keyToValue));
        }).forEach(aResult -> it(aPage).shouldHave(aResult));
    }

    private void verifyRemainingInAllCollections() {
        for (String aCollection : testCollections) {
            it(testDb.getCollection(aCollection).count()).shouldBe((long) 1);
            Map object3ofT1 = testDb.getCollection(aCollection).findOne(new BasicDBObject(Maps.map("id", 3))).toMap();
            it(object3ofT1.get("id")).shouldBe(3);
            it(object3ofT1.get("tenantId")).shouldBe("t1");
        }
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
            testDb.getCollection(aCollection).insert(new BasicDBObject(t1_1));
        }
    }
}