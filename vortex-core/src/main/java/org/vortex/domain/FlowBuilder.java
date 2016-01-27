package org.vortex.domain;

import org.vortex.basic.primitive.Callable1;
import org.vortex.query.ListQuery;

import java.util.List;
import java.util.function.Function;

public class FlowBuilder {
    private VTarget source;
    private ListQuery sourceQuery;
    private VTarget sink;
    private SourceSinkTransformer sourceSinkTransformer;

    public FlowBuilder withSource(VTarget source, ListQuery sourceQuery) {
        this.source = source;
        this.sourceQuery = sourceQuery;
        return this;
    }

    public FlowBuilder withSink(VTarget sink) {
        this.sink = sink;
        return this;
    }

    public FlowBuilder withTransformer(Callable1<QueryResult, VQuery> sourceSinkTransformer) {
        this.sourceSinkTransformer = new SourceSinkTransformer(sourceSinkTransformer);
        return this;
    }

    public FlowBuilder withTransformer(Function<QueryResult, List<VQuery>> sourceSinkTransformer) {
        this.sourceSinkTransformer = new SourceSinkTransformer(sourceSinkTransformer);
        return this;
    }

    public Flow build() {
        if (source == null || sourceQuery == null || sink == null || sourceSinkTransformer == null)
            throw new RuntimeException("invalid flow");
        return new Flow(source, sourceQuery, sink, sourceSinkTransformer);
    }
}
