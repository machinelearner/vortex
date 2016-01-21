package org.vortex.impl.target;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.help.Maps;
import org.vortex.help.Pair;
import org.vortex.help.Strings;
import org.vortex.query.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class OrientdbTarget extends GraphTarget {
    public static final String OR = " or ";
    public static final String AND = " and ";
    String GROUP_BEGIN = "( ";
    String GROUP_END = " )";
    private final OrientGraphFactory orientGraphFactory;
    private String[] vertices;

    public OrientdbTarget(Settings settings, OrientGraphFactory orientGraphFactory, String... vertices) {
        super(settings);
        this.vertices = vertices;
        this.orientGraphFactory = orientGraphFactory;
    }

    /**
     * Only Applicable with Remote connections!
     */
    public OrientdbTarget(Settings settings, String... vertices) {
        super(settings);
        this.vertices = vertices;
        orientGraphFactory = initialize(settings);
    }

    OrientGraphFactory initialize(Settings settings) {
        OrientGraph orientGraph = new OrientGraph(orientdbConnectionUrl(settings));
        orientGraph.shutdown();

        OrientGraphFactory graphFactory = new OrientGraphFactory(orientdbConnectionUrl(settings)).setupPool(Integer.parseInt(settings.get("orientdb.dbPool.min")), Integer.parseInt(settings.get("orientdb.dbPool.max")));
        return graphFactory;
    }

    private String orientdbConnectionUrl(Settings settings) {
        return String.format("%s/%s", settings.dbUrl("orientdb"), settings.get("orientdb.db.name"));
    }

    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }


    @Override
    public Callable<Result> delete(final BulkDeleteTaskQuery bulkDeleteTaskQuery) {

        return new Callable<Result>() {
            @Override
            public Result call() {
                try {
                    Map<String, String> resultOfBulkRemoveFromAllVertices = Maps.map(Stream.of(vertices).map(new Function<String, Pair<String, String>>() {
                        @Override
                        public Pair<String, String> apply(String vertex) {
                            OrientGraph graph = orientGraph();
                            String deleteQuery = Strings.format("delete vertex {entity} where {orConditions}",
                                    Maps.map("entity", vertex, "orConditions", orConditions(Arrays.asList(bulkDeleteTaskQuery.conditions()))));
                            LOGGER.info("SQL Query For Bulk Deletion: {}", deleteQuery);
                            int deleted = graph.command(new OCommandSQL(deleteQuery)).execute();
                            graph.commit();
                            return Pair.pair(vertex, new JSONObject(Maps.map("Deleted Vertices", deleted)).toJSONString());
                        }
                    }));
                    Result success = Result.success("Orientdb BulkDelete", Maps.<String, Object>map("query", bulkDeleteTaskQuery.toJson(), "result", resultOfBulkRemoveFromAllVertices));
                    LOGGER.info("Orientdb Delete successful with delete status: {}", resultOfBulkRemoveFromAllVertices);
                    LOGGER.debug("Orientdb Delete successful with result: {}", success.toJson());
                    return success;
                } catch (Exception e) {
                    Result failure = Result.failure("Orientdb Delete", Maps.map("query", bulkDeleteTaskQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Orientdb Delete failed with error", e);
                    LOGGER.debug("Orientdb Delete failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };
    }


    public List<Object> executeTextQuery(String selectQuery) {
        OrientGraph graph = orientGraph();

        Iterable<Element> results;
        Object executed = graph.command(new OCommandSQL(selectQuery)).execute();

        if (executed != null && Iterable.class.isInstance(executed)) results = (Iterable<Element>) executed;
        else results = Arrays.asList((Element) executed);

        List<Object> elements = StreamSupport.stream(results.spliterator(), false)
                .map(new Function<Element, Object>() {
                    @Override
                    public Object apply(final Element vertex) {
                        return Maps.map(vertex.getPropertyKeys().stream().map(key -> Pair.pair(key, vertex.getProperty(key))));
                    }
                })
                .collect(Collectors.toList());
        return elements;

    }

    protected int executeTextQueryDelete(String query) {
        OrientGraph graph = orientGraph();
        return (int) graph.command(new OCommandSQL(query)).execute();
    }

    @Override
    public Callable<Result> list(final ListQuery listQuery) {
        return new Callable<Result>() {
            @Override
            public Result call() {
                try {

                    String selectQuery = String.format("select from %s where %s %s %s %s",
                            listQuery.from(),
                            orConditions(Arrays.asList(listQuery.where())),
                            paginationOptimization(listQuery),
                            sortQuery(listQuery),
                            paginateQuery(listQuery)
                    );
                    selectQuery = selectQuery + " fetchplan out_*:-2 in_*:-2";

                    LOGGER.info("Listing Entities - for entity: {}", listQuery.from());
                    LOGGER.debug("Listing Entities - Generated SQL: {}", selectQuery);

                    List<Object> elements = executeTextQuery(selectQuery);
                    return Result.success("Orientdb List", Maps.map("query", listQuery.toJson(), "result", elements));

                } catch (Exception e) {

                    Result failure = Result.failure("Orientdb List", Maps.map("query", listQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Orientdb List failed with error", e);
                    LOGGER.debug("Orientdb List failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };
    }

    private String paginationOptimization(ListQuery listQuery) {
        String optimizedQuery = "";
        if (listQuery.pageStart() != null)
            optimizedQuery = String.format("AND (%s > '%s')", listQuery.pageStart().first(), listQuery.pageStart().second());
        return optimizedQuery;
    }

    private String paginateQuery(ListQuery listQuery) {
        String paginationQuery = "";
        if (listQuery.pageSize() != null) {
            Integer pageSize = listQuery.pageSize();
            paginationQuery += " LIMIT " + pageSize;

        }
        return paginationQuery;
    }

    private String sortQuery(final ListQuery listQuery) {
        final Map<ListQuery.SortOrder, String> orderMapping = Maps.map(ListQuery.SortOrder.ASC, "asc", ListQuery.SortOrder.DESC, "desc");
        List<Pair<String, Object>> sortClauses = listQuery.sort();
        if (sortClauses.isEmpty())
            return "";

        List<String> queries = sortClauses.stream().map(new Function<Pair<String, Object>, String>() {
            @Override
            public String apply(Pair<String, Object> sortClause) {
                //TODO: Pramod - WTH! orderBy fix!
                String orderBy;
                if (sortClause.second() instanceof ListQuery.SortOrder)
                    orderBy = orderMapping.get(sortClause.second());
                else
                    orderBy = (String) sortClause.second();
                return " " + sortClause.first() + " " + sortClause.second();
            }
        }).collect(Collectors.toList());

        return "ORDER BY " + StringUtils.join(queries, ", ");

    }


    @Override
    public Callable<Result> create(final CreateQuery query) {
        return new Callable<Result>() {
            @Override
            public Result call() {
                String createQuery = String.format("create vertex %s content %s", query.into(), new JSONObject(query.object()).toJSONString());
                LOGGER.info("Create entities - entity: {}, value: {}", query.into(), query.object());
                LOGGER.debug("Create entities - Generated SQL: {}", createQuery);
                try {
                    List<Object> results = executeTextQuery(createQuery);
                    return Result.success("Orientdb create", Maps.map("query", query.toJson(), "result", results.get(0)));
                } catch (Exception e) {
                    Result failure = Result.failure("Orientdb create", Maps.map("query", query.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Orientdb create failed with error", e);
                    LOGGER.debug("Orientdb create failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };
    }

    @Override
    public Callable<Result> update(final UpdateQuery updateQuery) {
        return new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                try {
                    String upsert = updateQuery.upsert() ? "UPSERT" : "";
                    String entity = updateQuery.into();
                    String setConditions = setConditions(updateQuery.object());
                    String whereConditions = orConditions(Arrays.asList((updateQuery.where())));
                    String update = String.format("update %s SET %s %s WHERE %s", entity, setConditions, upsert, whereConditions);

                    LOGGER.info("Update entity - Entity: {}, setConditions: {}, whereConditions: {}", entity, setConditions, whereConditions);
                    LOGGER.debug("Update entity - Generated SQL: {}", update);

                    int results = executeTextQueryDelete(update);
                    return Result.success("Orientdb create", Maps.<String, Object>map("query", updateQuery.toJson(), "result", results));

                } catch (Exception e) {
                    Result failure = Result.failure("Orientdb update", Maps.map("query", updateQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Orientdb create failed with error", e);
                    LOGGER.debug("Orientdb create failed with result: {}", failure.toJson());
                    return failure;

                }
            }
        };
    }

    @Override
    public Callable<Result> count(final ListQuery listQuery) {
        return new Callable<Result>() {
            @Override
            public Result call() {
                try {

                    String selectQuery = String.format("select count(*) from %s where %s %s", listQuery.from(), orConditions(Arrays.asList(listQuery.where())), sortQuery(listQuery));
                    selectQuery = selectQuery + " fetchplan out_*:-2 in_*:-2";

                    LOGGER.info("Listing Entities - for entity: {}", listQuery.from());
                    LOGGER.debug("Listing Entities - Generated SQL: {}", selectQuery);

                    List<Object> elements = executeTextQuery(selectQuery);
                    return Result.success("Orientdb List", Maps.map("query", listQuery.toJson(), "result", elements.stream().findFirst().get()));

                } catch (Exception e) {

                    Result failure = Result.failure("Orientdb List", Maps.map("query", listQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Orientdb List failed with error", e);
                    LOGGER.debug("Orientdb List failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };
    }

    @Override
    public Callable<Result> delete(DeleteQuery deleteQuery) {
        return null;
    }

    @Override
    public Callable<Result> connectedList(final JoinQuery query) {
        return new Callable<Result>() {

            @Override
            public Result call() throws Exception {
                String selectQuery = String.format("select expand(%s('%s')) from %s where %s", query.direction(), query.relation(), query.entity(), orConditions(query.conditions()));
                selectQuery = selectQuery + " fetchplan out_*:-2 in_*:-2";
                LOGGER.info("Listing Connected Entities - for entity: {}, following relation: {}", query.entity(), query.relation());
                LOGGER.debug("Listing Connected Entities - Generated SQL: {}", selectQuery);
                try {
                    List<Object> results = executeTextQuery(selectQuery);
                    return Result.success("Orientdb List", Maps.map("query", query.toJson(), "result", results));
                } catch (Exception e) {
                    Result failure = Result.failure("Orientdb connected list", Maps.map("query", query.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Orientdb connected list failed with error", e);
                    LOGGER.debug("Orientdb connected list failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };
    }

    @Override
    public Callable<Result> relate(final RelateQuery query) {
        return new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                String fromSelectQuery = String.format("select from %s where %s", query.fromEntity(), orConditions(query.fromConditions()));
                String toSelectQuery = String.format("select from %s where %s", query.toEntity(), orConditions(Arrays.asList(query.toCondition())));
                String createEdgeQuery = String.format("create edge %s from (%s) to (%s);", query.relation(), fromSelectQuery, toSelectQuery);
                LOGGER.info("Relate entity - fromEntity: {}, toEntity: {}, relation: {}, relating to: {}", query.fromEntity(), query.toEntity(), query.relation(), query.toCondition());
                LOGGER.debug("Relate entity - Generated SQL: {}", createEdgeQuery);
                try {
                    List<Object> results = executeTextQuery(createEdgeQuery);
                    return Result.success("Orientdb List", Maps.map("query", query.toJson(), "result", results));
                } catch (Exception e) {
                    Result failure = Result.failure("Orientdb relate", Maps.map("query", query.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Orientdb relate  failed with error", e);
                    LOGGER.debug("Orientdb relate  failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };
    }

    public OrientGraph orientGraph() {
        return orientGraphFactory.getTx();
    }

    public String andGroup(Map<String, Object> conditions) {
        String andGroup = GROUP_BEGIN;
        Iterator<Map.Entry<String, Object>> entries = conditions.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Object> keyValue = entries.next();
            andGroup += keyValue.getKey() + "='" + keyValue.getValue() + "'";
            if (entries.hasNext()) andGroup += AND;
        }
        if (andGroup.equalsIgnoreCase(GROUP_BEGIN))
            throw new RuntimeException("Malformed Query with empty conditions");
        andGroup += GROUP_END;
        return andGroup;
    }

    public String orGroups(List<String> groupedStrings) {
        return StringUtils.join(groupedStrings, OR);
    }

    public String orConditions(List<Map<String, Object>> conditions) {
        String orGroups = orGroups(conditions.stream()
                .filter(other -> !other.isEmpty())
                .map(this::andGroup)
                .collect(Collectors.toList()));

        if (orGroups.isEmpty())
            return "(1=1)";
        return orGroups;
    }

    public String set(Map<String, Object> setMaps) {
        String andGroup = "";
        Iterator<Map.Entry<String, Object>> entries = setMaps.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Object> keyValue = entries.next();
            andGroup += keyValue.getKey() + "='" + keyValue.getValue() + "'";
            if (entries.hasNext()) andGroup += ",";
        }
        return andGroup;

    }

    public String setConditions(Map<String, Object> conditions) {
        String conditionsQuery = set(conditions);
        return conditionsQuery.length() == 0 ? "(1=1)" : conditionsQuery;
    }
}
