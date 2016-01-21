package org.vortex.impl.target;

import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.domain.VTarget;
import org.vortex.query.*;

import java.util.concurrent.Callable;

public class ElasticsearchTarget extends VTarget {
    public ElasticsearchTarget(Settings settings) {
        super(settings);
    }

    @Override
    public String info() {
        return null;
    }

    @Override
    public Callable<Result> delete(BulkDeleteTaskQuery bulkDeleteTaskQuery) {
        return null;
    }

    @Override
    public Callable<Result> list(ListQuery listQuery) {
        return null;
    }

    @Override
    public Callable<Result> create(CreateQuery createQuery) {
        return null;
    }

    @Override
    public Callable<Result> update(UpdateQuery updateQuery) {
        return null;
    }

    @Override
    public Callable<Result> count(ListQuery listQuery) {
        return null;
    }

    @Override
    public Callable<Result> delete(DeleteQuery deleteQuery) {
        return null;
    }

//    private List<String> indices;
//    private Client transportClient;
//
//    public ElasticsearchTarget(Settings settings, List<String> indices) {
//        this(initialize(settings), indices);
//    }
//
//    public ElasticsearchTarget(Client transportClient, List<String> indices) {
//        super(Settings.defaults());
//        this.transportClient = transportClient;
//        this.indices = indices;
//    }
//
//    public static TransportClient initialize(Settings settings) {
//        String elasticsearchCommaSeparatedUrl = settings.dbUrl("elasticsearch");
//        TransportClient transportClient = new TransportClient();
//        for (String hostAndPort : elasticsearchCommaSeparatedUrl.split(",")) {
//            String host = hostAndPort.split(":")[0];
//            String port = hostAndPort.split(":")[1];
//            transportClient
//                    .addTransportAddress(new InetSocketTransportAddress(host, Integer.parseInt(port)));
//        }
//        return transportClient;
//    }
//
//    @Override
//    public String info() {
//        return this.getClass().getSimpleName();
//    }
//
//    @Override
//    public Callable<Result> delete(final BulkDeleteTaskQuery bulkDeleteTaskQuery) {
//        return new Callable<Result>() {
//            @Override
//            public Result call() {
//                try {
//                    Map resultOfDeleteIndex = Maps.map(Sequences.sequence(indices).map(new Function1<String, Pair<String, String>>() {
//                        public Pair<String, String> call(String index) {
//                            DeleteIndexResponse deleteIndexResponse = transportClient.admin().indices().prepareDelete(index).execute().actionGet();
//                            return Pair.pair(index, deleteIndexResponse.toString());
//                        }
//                    }));
//                    Result success = Result.success("Elasticsearch Delete", Maps.<String, Object>map("query", bulkDeleteTaskQuery.toJson(), "result", resultOfDeleteIndex));
//                    LOGGER.info("Elasticsearch Delete successful for indices: {}", indices);
//                    LOGGER.debug("Elasticsearch Delete successful with result: {}", success.toJson());
//                    return success;
//                } catch (Exception e) {
//                    Result failure = Result.failure("Elasticsearch Delete", Maps.map("query", bulkDeleteTaskQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
//                    LOGGER.error("Elasticsearch Delete failed with error", e);
//                    LOGGER.debug("Elasticsearch Delete failed with result: {}", failure.toJson());
//                    return failure;
//                }
//
//            }
//        };
//    }
//
//    @Override
//    public Callable<Result> list(ListQuery listQuery) {
//        return null;
//    }
//
//    @Override
//    public Callable<Result> create(CreateQuery createQuery) {
//        return null;
//    }
//
//    @Override
//    public Callable<Result> update(UpdateQuery updateQuery) {
//        return null;
//    }
//
//    @Override
//    public Callable<Result> count(ListQuery listQuery) {
//        return null;
//    }
//
//    @Override
//    public Callable<Result> delete(final DeleteQuery deleteQuery) {
//        return new Callable<Result>() {
//            @Override
//            public Result call() {
//                try {
//                    Map resultOfDeleteIndex = Maps.map(Sequences.sequence(indices).map(new Function1<String, Pair<String, String>>() {
//                        public Pair<String, String> call(String index) {
//                            final BoolQueryBuilder query = boolQuery();
//                            final Map<String, Object> whereClauses = deleteQuery.where();
//                            Sequences.sequence(whereClauses.keySet()).forEach(new Callable1<String, String>() {
//                                @Override
//                                public String call(String key) throws Exception {
//                                    query.must(termQuery(key, whereClauses.get(key)));
//                                    return null;
//                                }
//                            });
//                            DeleteByQueryResponse deleteIndexResponse = transportClient.prepareDeleteByQuery(index) .setTypes(deleteQuery.from())
//                                    .setQuery(query)
//                                    .execute()
//                                    .actionGet();
//                            return Pair.pair(index, deleteIndexResponse.toString());
//                        }
//                    }));
//                    Result success = Result.success("Elasticsearch Delete", Maps.<String, Object>map("query", deleteQuery.toJson(), "result", resultOfDeleteIndex));
//                    LOGGER.info("Elasticsearch Delete successful for indices: {}", indices);
//                    LOGGER.debug("Elasticsearch Delete successful with result: {}", success.toJson());
//                    return success;
//                } catch (Exception e) {
//                    Result failure = Result.failure("Elasticsearch Delete", Maps.map("query", deleteQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
//                    LOGGER.error("Elasticsearch Delete failed with error", e);
//                    LOGGER.debug("Elasticsearch Delete failed with result: {}", failure.toJson());
//                    return failure;
//                }
//
//            }
//        };
//    }
}
