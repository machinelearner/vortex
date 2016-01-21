package org.vortex.domain;


import org.vortex.help.Callable1;
import org.vortex.help.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class SourceSinkTransformer {
    private Function<QueryResult, List<VQuery>> transformer;

    public SourceSinkTransformer(Function<QueryResult, List<VQuery>> transformer) {
        this.transformer = transformer;
    }

    public SourceSinkTransformer(final Callable1<QueryResult, VQuery> transformer) {
        this.transformer = queryResult -> {
            try {
                return Arrays.asList(transformer.call(queryResult));
            } catch (Exception e) {
                e.printStackTrace();
                return Lists.list();
            }
        };
    }

    public Function<QueryResult, List<VQuery>> transformer() {
        return transformer;
    }

}
