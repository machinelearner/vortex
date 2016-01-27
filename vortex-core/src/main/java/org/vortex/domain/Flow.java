package org.vortex.domain;

import org.vortex.basic.StructuredLog;
import org.vortex.basic.primitive.Maps;
import org.vortex.basic.primitive.Pair;
import org.vortex.query.ListQuery;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static org.vortex.basic.primitive.Pair.pair;


/**
 * Responsible for representing what a flow means and the interactions of source, sink and flow of information from source to sink
 */
public class Flow {
    private Map<String, Object> metadata;
    private final VTarget source;
    private final ListQuery sourceQuery;
    private final VTarget sink;
    private final SourceSinkTransformer sourceSinkTransformation;
    protected StructuredLog LOGGER = new StructuredLog();
    private Function<VQuery, Result> ExecuteAtSink = new Function<VQuery, Result>() {
        @Override
        public Result apply(VQuery taskQuery) {
            try {
                return sink.execute(taskQuery).call();
            } catch (Exception e) {
                LOGGER.error(e, Pair.of("stage", "sink"));
                return Result.failure("Execution at sink failed", Maps.map());
            }
        }
    };
    private Function<Map<String, Object>, QueryResult> resultToQueryResult = new Function<Map<String, Object>, QueryResult>() {
        @Override
        public QueryResult apply(Map<String, Object> aResult) {
            return new QueryResult(sourceQuery, aResult);
        }
    };

    public Flow(final VTarget source, final ListQuery sourceQuery, VTarget sink, SourceSinkTransformer sourceSinkTransformation) {
        this.source = source;
        this.sourceQuery = sourceQuery;
        this.sink = sink;
        this.sourceSinkTransformation = sourceSinkTransformation;
        this.metadata = Maps.map();
    }

    public Callable<Result> execute() {
        return new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                Map<String, Object> lastResultOfPage = null;
                try {
                    Result resultCount = source.count(sourceQuery).call();
                    Long count = (Long) ((Map) resultCount.result().get("result")).get("count");
                    for (int i = 0; i < count; i += sourceQuery.pageSize()) {
                        Result result = source.execute(sourceQuery).call();
                        List<Map<String, Object>> aPage = (List) result.result().get("result");
                        aPage.stream()
                                .map(resultToQueryResult)
                                .map(sourceSinkTransformation.transformer())
                                .flatMap(Collection::stream)
                                .map(ExecuteAtSink)
                                .forEach(aResult -> {
                                    LOGGER.info(pair("stage", "sink"), pair("state", result.isSuccess()));
                                    LOGGER.debug(pair("result", result.toJson()));
                                });
                        lastResultOfPage = aPage.stream().reduce((a, b) -> b).orElse(Maps.<String, Object>map());
                        Object lastPagePaginationCriterionValue = lastResultOfPage.get(sourceQuery.pageStartField());
                        sourceQuery.pageStart(sourceQuery.pageStartField(), lastPagePaginationCriterionValue);
                    }
                    return Result.success("Flow", Maps.<String, Object>map(metadata(), pair("info", info()), pair("result", Maps.map("count", count, "lastResult", lastResultOfPage))));
                } catch (Exception e) {
                    Result failure = Result.failure("Flow", Maps.<String, Object>map(metadata(), pair("info", info()), pair("result", Maps.map("lastResult", lastResultOfPage)), pair("errors", Arrays.asList(e.getMessage()))));
                    LOGGER.error(e, pair("stage", "flow"));
                    LOGGER.debug(pair("stage", "flow"), pair("result", failure.toJson()));
                    return failure;
                }
            }
        };
    }

    public Map<String, String> info() {
        return Maps.map("Source", source.info(), "Sink", sink.info(), "SourceQuery", sourceQuery.info());
    }

    public Flow withMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }
}
