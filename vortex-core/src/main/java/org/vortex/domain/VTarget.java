package org.vortex.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vortex.Settings;
import org.vortex.basic.primitive.Maps;
import org.vortex.query.*;

import java.util.Arrays;
import java.util.concurrent.Callable;

public abstract class VTarget {

    protected Settings settings;
    @Deprecated
    protected Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public VTarget(Settings settings) {
        this.settings = settings;
    }

    abstract public String info();

    public Callable<Result> execute(VQuery taskQuery) {
        LOGGER.info("Executing Query {} against target {}", taskQuery.info(), this.info());
        LOGGER.debug("Query Details {} against target {}", taskQuery.toJson(), this.info());
        return taskQuery.executeAgainst(this);
    }

    public static Callable<Result> failureCallable() {
        return () -> Result.failure(Maps.map("errors", Arrays.asList("Query Type not found")));
    }

    public static Callable<Result> successCallable() {
        return () -> Result.success("No Op");
    }

    abstract public Callable<Result> delete(BulkDeleteTaskQuery bulkDeleteTaskQuery);

    public abstract Callable<Result> list(ListQuery listQuery);

    public abstract Callable<Result> create(CreateQuery createQuery);

    public abstract Callable<Result> update(UpdateQuery updateQuery);

    public abstract Callable<Result> count(ListQuery listQuery);

    public abstract Callable<Result> delete(DeleteQuery deleteQuery);

}
