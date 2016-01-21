package org.vortex.impl.target;

import com.mongodb.*;
import org.bson.BSONObject;
import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.domain.VTarget;
import org.vortex.executor.ExecutorException;
import org.vortex.help.Maps;
import org.vortex.help.Pair;
import org.vortex.query.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class MongoTarget extends VTarget {
    private static final boolean MULTI = false;
    private final String[] collections;
    private MongoClient mongoClient;
    private DB mongoDatabase;

    public MongoTarget(Settings settings) {
        super(settings);
        mongoClient = initialize(settings);
        mongoDatabase = mongoClient.getDB(settings.dbName("mongo"));

        //Deprecated
        collections = new String[0];
    }

    MongoClient initialize(Settings settings) {
        try {
            mongoClient = new MongoClient(new MongoClientURI(settings.dbUrl("mongo")));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new ExecutorException(e.getMessage());
        }
        return mongoClient;
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
                    Map<String, String> resultOfBulkRemoveFromAllCollection = Arrays.stream(collections)
                            .map(collection -> {
                                final BasicDBObject query = mongoQueryFromConditions(bulkDeleteTaskQuery.conditions());
                                LOGGER.info("Mongo Query For Bulk Deletion: {}", query);
                                WriteResult bulkRemove = mongoDatabase.getCollection(collection).remove(query);
                                return Pair.pair(collection, bulkRemove.toString());
                            })
                            .collect(Collectors.toMap(Pair::first, Pair::second));
                    Result success = Result.success("Mongo BulkDelete", Maps.<String, Object>map("query", bulkDeleteTaskQuery.toJson(), "result", resultOfBulkRemoveFromAllCollection));
                    LOGGER.info("Mongo Delete successful for collections: {}", collections);
                    LOGGER.debug("Mongo Delete successful with result: {}", success.toJson());
                    return success;
                } catch (Exception e) {
                    Result failure = Result.failure("Mongo Delete", Maps.map("query", bulkDeleteTaskQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Mongo Delete failed with error", e);
                    LOGGER.debug("Mongo Delete failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };
    }

    @Override
    public Callable<Result> list(final ListQuery listQuery) {
        return new Callable<Result>() {
            @Override
            public Result call() {
                try {
                    final BasicDBObject query = mongoQueryFromConditions(listQuery.where());
                    paginationOptimization(query, listQuery);
                    LOGGER.info("Mongo Listing Query: {}", query);
                    DBCursor dbObjects = mongoDatabase.getCollection(listQuery.from()).find(query).sort(orderByClauses(listQuery)).limit(listQuery.pageSize());
                    List<Map> listing = StreamSupport.stream(dbObjects.spliterator(), false)
                            .map(BSONObject::toMap)
                            .collect(Collectors.toList());

                    Result success = Result.success("Mongo Listing Query", Maps.map("query", listQuery.toJson(), "result", listing));
                    LOGGER.info("Mongo List successful for collection: {}", listQuery.from());
                    LOGGER.debug("Mongo List successful with result: {}", success.toJson());
                    return success;
                } catch (Exception e) {
                    Result failure = Result.failure("Mongo List", Maps.map("query", listQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Mongo List failed with error", e);
                    LOGGER.debug("Mongo List failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };
    }

    @Override
    public Callable<Result> create(final CreateQuery createQuery) {
        return new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                try {
                    LOGGER.info("Mongo Create Query: {}", createQuery.conditions());
                    WriteResult writeResult = mongoDatabase.getCollection(createQuery.into()).insert(new BasicDBObject(createQuery.object()));
                    Result success = Result.success("Mongo Create Query", Maps.map("query", createQuery.toJson(), "result", writeResult));

                    LOGGER.info("Mongo Create successful for collections: {}", createQuery.into());
                    LOGGER.debug("Mongo Create successful with result: {}", success.toJson());
                    return success;
                } catch (Exception e) {
                    Result failure = Result.failure("Mongo Create", Maps.map("query", createQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Mongo Create failed with error", e);
                    LOGGER.debug("Mongo Create failed with result: {}", failure.toJson());
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
                    LOGGER.info("Mongo Update Query: {}", updateQuery.conditions());
                    BasicDBObject objectToUpdateQuery = mongoQueryFromConditions(updateQuery.where());
                    WriteResult writeResult = mongoDatabase.getCollection(updateQuery.into()).update(objectToUpdateQuery, new BasicDBObject(updateQuery.object()), updateQuery.upsert(), MULTI);
                    Result success = Result.success("Mongo Update Query", Maps.map("query", updateQuery.toJson(), "result", writeResult));

                    LOGGER.info("Mongo Update successful for collections: {}", updateQuery.into());
                    LOGGER.debug("Mongo Update successful with result: {}", success.toJson());
                    return success;
                } catch (Exception e) {
                    Result failure = Result.failure("Mongo Update", Maps.map("query", updateQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Mongo Update failed with error", e);
                    LOGGER.debug("Mongo Update failed with result: {}", failure.toJson());
                    return failure;
                }
            }
        };

    }

    @Override
    public Callable<Result> count(final ListQuery listQuery) {
        return new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                try {
                    final BasicDBObject query = mongoQueryFromConditions(listQuery.where());
                    LOGGER.info("Mongo Count Query: {}", query);
                    long dbObjects = mongoDatabase.getCollection(listQuery.from()).count(query);

                    Result success = Result.success("Mongo Count Query", Maps.<String, Object>map("query", listQuery.toJson(), "result", Maps.map("count", dbObjects)));
                    LOGGER.info("Mongo Count successful for collection: {}", listQuery.from());
                    LOGGER.debug("Mongo Count successful with result: {}", success.toJson());
                    return success;
                } catch (Exception e) {
                    Result failure = Result.failure("Mongo Count", Maps.map("query", listQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                    LOGGER.error("Mongo Count failed with error", e);
                    LOGGER.debug("Mongo Count failed with result: {}", failure.toJson());
                    return failure;
                }

            }
        };
    }

    @Override
    public Callable<Result> delete(DeleteQuery deleteQuery) {
        return null;
    }

    private void paginationOptimization(BasicDBObject query, ListQuery listQuery) {
        if (listQuery.pageStart() != null)
            query.append(listQuery.pageStart().first(), new BasicDBObject("$gt", listQuery.pageStart().second()));
    }

    private BasicDBObject orderByClauses(ListQuery listQuery) {
        final Map<ListQuery.SortOrder, Integer> orderMapping = Maps.map(ListQuery.SortOrder.ASC, 1, ListQuery.SortOrder.DESC, -1);
        Map<String, Integer> result = listQuery.sort().stream()
                .map(sortOrder -> {
                    if (sortOrder.second() instanceof ListQuery.SortOrder)
                        return Pair.pair(sortOrder.first(), orderMapping.get(sortOrder.second()));
                    return Pair.pair(sortOrder.first(), Integer.parseInt((String) sortOrder.second()));

                }).collect(Collectors.toMap(Pair::first, Pair::second));
        return new BasicDBObject(result);
    }

    private BasicDBObject mongoQueryFromConditions(Map<String, Object> conditions) {
        final BasicDBObject query = new BasicDBObject();
        for (Map.Entry<String, Object> condition : conditions.entrySet()) {
            query.append(condition.getKey(), condition.getValue());
        }
        return query;
    }
}
