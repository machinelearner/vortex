package org.vortex.impl.target;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.help.Maps;
import org.vortex.query.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static name.mlnkrishnan.shouldJ.ShouldJ.it;

public class OrientdbTargetTest {

    private static final int PAGE_SIZE = 4;
    private OrientdbTarget orientdbTarget;
    private String[] testVertices;
    private OrientGraph orientGraph;

    @Before
    public void setUp() {
        testVertices = new String[]{"bla", "blu_bla"};
        orientdbTarget = new OrientdbTarget(Settings.defaults(), testVertices);
        orientGraph = orientdbTarget.orientGraph();

        orientGraph.setAutoStartTx(false);
        orientGraph.commit();
        for (String vertex : testVertices) {
            orientGraph.createVertexType(vertex);
        }
        orientGraph.setAutoStartTx(true);
        orientGraph.commit();
    }

    @After
    public void tearDown() {
        orientGraph.drop();
    }

    @Test
    public void shouldDeleteGivenVerticesAndConditions() throws Exception {
        insertSeedInTestVertices();
        BulkDeleteTaskQuery deleteAllRecordsOfATenant = new BulkDeleteTaskQuery()
                .condition("tenantId", "dev");

        Result deleteMongoResult = orientdbTarget.delete(deleteAllRecordsOfATenant).call();

        it(deleteMongoResult.isSuccess()).shouldBeTrue();
        verifyRemainingInAllCollections();

    }

    @Test
    public void shouldListAllTheVerticesFromCondition() throws Exception {
        insertSeedInTestVertices();
        ListQuery query = new ListQuery().where("tenantId", "dev").from("bla");
        Result result = orientdbTarget.list(query).call();

        it(result.isSuccess()).shouldBeTrue();
        List resultsList = (List) result.result().get("result");
        it(resultsList.size()).shouldBe(2);
        it(resultsList).shouldHaveAll(Maps.map("_id", 1, "tenantId", "dev"), Maps.map("_id", 2, "tenantId", "dev"));

    }

    @Test
    public void shouldCreateVertices() throws Exception {
        CreateQuery query = new CreateQuery().into("bla").object(Maps.<String, Object>map("_id", 1, "tenantId", "devTest"));
        orientdbTarget.create(query).call();

        ListQuery listQuery = new ListQuery().where("tenantId", "devTest").from("bla");
        Result result = orientdbTarget.list(listQuery).call();

        it(result.isSuccess()).shouldBeTrue();
        List resultsList = (List) result.result().get("result");
        it(resultsList.size()).shouldBe(1);
        it(resultsList).shouldHaveAll(Maps.map("_id", 1, "tenantId", "devTest"));
    }

    @Test
    @Ignore
    public void shouldCreateRelateBaseAndRelatedAndListInConnectedQuery() throws Exception {
        orientGraph.createEdgeType("relation");
        orientGraph.createVertexType("base");
        orientGraph.createVertexType("related");

        CreateQuery baseElementQuery = new CreateQuery().into("base").object(Maps.<String, Object>map("_id", 1, "tenantId", "devTest"));
        orientdbTarget.create(baseElementQuery).call();

        CreateQuery relatedElement = new CreateQuery().into("related").object(Maps.<String, Object>map("_id", 2, "tenantId", "devTest"));
        orientdbTarget.create(relatedElement).call();

        relatedElement = new CreateQuery().into("related").object(Maps.<String, Object>map("_id", 3, "tenantId", "devTest"));
        orientdbTarget.create(relatedElement).call();

        RelateQuery relationQuery = new RelateQuery().relation("base", "relation", "related").fromCondition(Maps.<String, Object>map("_id", 1, "tenantId", "devTest")).toCondition(Maps.<String, Object>map("_id", 2, "tenantId", "devTest"));
        orientdbTarget.relate(relationQuery).call();

        relationQuery = new RelateQuery().relation("base", "relation", "related").fromCondition(Maps.<String, Object>map("_id", 1,"tenantId", "devTest")).toCondition(Maps.<String, Object>map("_id", 3, "tenantId", "devTest"));
        orientdbTarget.relate(relationQuery).call();

        JoinQuery connectedQuery = new JoinQuery().relation("relation", "base").condition(Maps.<String, Object>map("_id", 1, "tenantId", "devTest")).out();
        Result result = orientdbTarget.connectedList(connectedQuery).call();

        it(result.isSuccess()).shouldBeTrue();
    }

    @Test
    public void shouldUpdateRecord() throws Exception {
        CreateQuery query = new CreateQuery().into("bla").object(Maps.<String, Object>map("_id", 1, "tenantId", "devTest"));
        orientdbTarget.create(query).call();

        UpdateQuery update = new UpdateQuery().into("bla").where("tenantId", "devTest").object(Maps.<String, Object>map("tenantId", "dev"));
        orientdbTarget.update(update).call();

        ListQuery listQuery = new ListQuery().where("tenantId", "dev").from("bla");
        Result result = orientdbTarget.list(listQuery).call();


        it(result.isSuccess()).shouldBeTrue();
        List resultsList = (List) result.result().get("result");
        it(resultsList.size()).shouldBe(1);
        it(resultsList).shouldHaveAll(Maps.map("_id", 1, "tenantId", "dev"));

    }

    @Test
    public void shouldUpsertRecordWhenRecordNotExistsAndUpsertIsEnabled() throws Exception {
        UpdateQuery update = new UpdateQuery().into("bla").where("_id", 1).object(Maps.<String, Object>map("_id", "1", "tenantId", "devTest", "name", "user1")).upsert(true);
        orientdbTarget.update(update).call();

        ListQuery listQuery = new ListQuery().where("tenantId", "devTest").from("bla");
        Result result = orientdbTarget.list(listQuery).call();


        it(result.isSuccess()).shouldBeTrue();
        List resultsList = (List) result.result().get("result");
        it(resultsList.size()).shouldBe(1);
        it(resultsList).shouldHaveAll(Maps.map("_id", "1", "tenantId", "devTest", "name", "user1"));

    }

    @Test
    public void shouldUpdateRecordWhenRecordExistsAndUpsertIsEnabled() throws Exception {
        CreateQuery query = new CreateQuery().into("bla").object(Maps.<String, Object>map("_id", "1", "tenantId", "devTest", "name", "user"));
        orientdbTarget.create(query).call();

        UpdateQuery update = new UpdateQuery().into("bla").where("_id", 1).object(Maps.<String, Object>map("_id", "1", "tenantId", "devTest", "name", "user1")).upsert(true);
        orientdbTarget.update(update).call();

        ListQuery listQuery = new ListQuery().where("tenantId", "devTest").from("bla");
        Result result = orientdbTarget.list(listQuery).call();


        it(result.isSuccess()).shouldBeTrue();
        List resultsList = (List) result.result().get("result");
        it(resultsList.size()).shouldBe(1);
        it(resultsList).shouldHaveAll(Maps.<String, Object>map("_id", "1", "tenantId", "devTest", "name", "user1"));

    }

    @Test
    public void shouldGetCount() throws Exception {
        CreateQuery query = new CreateQuery().into("bla").object(Maps.<String, Object>map("_id", 1, "tenantId", "devTest"));
        orientdbTarget.create(query).call();

        ListQuery listQuery = new ListQuery().where("tenantId", "devTest").from("bla");
        Result result = orientdbTarget.count(listQuery).call();
        Map resultCount = (Map) result.result().get("result");

        it((Long) resultCount.get("count")).shouldBe(new Long(1));
    }

    @Test
    public void shouldListRecordsInACollection_PageSize4() throws Exception {
        insertPaginatedSeedInTestVertices();
        ListQuery listFirstPageOfDevTenant = new ListQuery()
                .from("bla")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageSize(PAGE_SIZE)
                .pageStart("_id", 0);
        Result result = orientdbTarget.list(listFirstPageOfDevTenant).call();
        it(result.isSuccess()).shouldBeTrue();
        List<Map<String, Object>> results = (List<Map<String, Object>>) result.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, 1, PAGE_SIZE);
    }

    @Test
    public void shouldListRecordsInACollection_PageSize4_PageNumber2() throws Exception {
        insertPaginatedSeedInTestVertices();
        ListQuery listFirstPageOfDevTenant = new ListQuery()
                .from("bla")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageSize(PAGE_SIZE)
                .pageStart("_id", 4);
        Result result = orientdbTarget.list(listFirstPageOfDevTenant).call();
        it(result.isSuccess()).shouldBeTrue();
        List<Map<String, Object>> results = (List<Map<String, Object>>) result.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, 5, PAGE_SIZE);
    }

    @Test
    public void shouldListRecordsInACollection_PageSize4_PageNumber1_2_3() throws Exception {
        insertPaginatedSeedInTestVertices();
        ListQuery listExpertiseFirstPageOfDevTenant = new ListQuery()
                .from("bla")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageSize(PAGE_SIZE);
        ListQuery listExpertiseSecondPageOfDevTenant = new ListQuery()
                .from("bla")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageStart("_id", PAGE_SIZE)
                .pageSize(PAGE_SIZE);
        ListQuery listExpertiseThirdPageOfDevTenant = new ListQuery()
                .from("bla")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageStart("_id", 2 * PAGE_SIZE)
                .pageSize(PAGE_SIZE);
        Result aPage = orientdbTarget.list(listExpertiseFirstPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        List results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, 1, PAGE_SIZE);

        aPage = orientdbTarget.list(listExpertiseSecondPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, 5, PAGE_SIZE);

        aPage = orientdbTarget.list(listExpertiseThirdPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(4);
        verifyPage(results, 9, PAGE_SIZE);
    }

    @Test
    public void shouldListRecordsInACollection_PageSize4_PageNumber4_With2Records() throws Exception {
        insertPaginatedSeedInTestVertices();
        ListQuery listExpertiseFourthPageOfDevTenant = new ListQuery()
                .from("bla")
                .where("tenantId", "dev")
                .sortBy("_id", ListQuery.SortOrder.ASC)
                .pageStart("_id", 3 * PAGE_SIZE)
                .pageSize(PAGE_SIZE);
        Result aPage = orientdbTarget.list(listExpertiseFourthPageOfDevTenant).call();
        it(aPage.isSuccess()).shouldBeTrue();
        List results = (List) aPage.result().get("result");
        it(results).shouldBeOfSize(2);
        verifyPage(results, 13, PAGE_SIZE);
        
    }


    private void verifyPage(List<Map<String, Object>> results, int i, int pageSize) {
        for (Map result : results) {
            it(result.get("_id")).shouldBe(i++);
        }
    }

    private void insertSeedInTestVertices() {
        for (String vertex : testVertices) {
            orientGraph.addVertex("class:" + vertex, Maps.map("_id", 1, "tenantId", "dev"));
            orientGraph.addVertex("class:" + vertex, Maps.map("_id", 2, "tenantId", "dev"));
            orientGraph.addVertex("class:" + vertex, Maps.map("_id", 3, "tenantId", "t1"));
            orientGraph.addVertex("class:" + vertex, Maps.map("_id", 3, "tenantId", "t2"));
        }
        orientGraph.commit();
    }

    private void insertPaginatedSeedInTestVertices() {
        String vertex = testVertices[0];
        for (int i = 0; i < 14; i++) {
            orientGraph.addVertex("class:" + vertex, Maps.map("_id", i + 1, "tenantId", "dev"));
        }
        orientGraph.commit();
    }

    private void verifyRemainingInAllCollections() {
        for (String vertex : testVertices) {
            List<Vertex> allVertices = StreamSupport.stream(orientGraph.getVerticesOfClass(vertex).spliterator(), false).collect(Collectors.toList());
            it(allVertices.size()).shouldBe(2);
            it(allVertices.get(0).<Integer>getProperty("_id")).shouldBe(3);
            it(allVertices.get(0).<String>getProperty("tenantId")).shouldBe("t1");
            it(allVertices.get(1).<Integer>getProperty("_id")).shouldBe(3);
            it(allVertices.get(1).<String>getProperty("tenantId")).shouldBe("t2");
        }
    }

}